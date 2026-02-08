use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use kitedb::core::single_file::{close_single_file, open_single_file, SingleFileOpenOptions};
use kitedb::metrics::{
  collect_metrics_single_file, collect_replication_metrics_otel_json_single_file,
  collect_replication_metrics_otel_protobuf_single_file,
  collect_replication_metrics_prometheus_single_file, push_replication_metrics_otel_grpc_payload,
  push_replication_metrics_otel_json_payload,
  push_replication_metrics_otel_json_payload_with_options,
  push_replication_metrics_otel_protobuf_payload, render_replication_metrics_prometheus,
  OtlpHttpPushOptions, OtlpHttpTlsOptions,
};
use kitedb::replication::types::ReplicationRole;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
  MetricsService as OtelMetricsService, MetricsServiceServer as OtelMetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest as OtelExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse as OtelExportMetricsServiceResponse;
use prost::Message;

fn open_primary(
  path: &std::path::Path,
  sidecar: &std::path::Path,
  segment_max_bytes: u64,
  retention_min_entries: u64,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_sidecar_path(sidecar)
      .replication_segment_max_bytes(segment_max_bytes)
      .replication_retention_min_entries(retention_min_entries),
  )
}

fn open_replica(
  replica_path: &std::path::Path,
  source_db_path: &std::path::Path,
  local_sidecar: &std::path::Path,
  source_sidecar: &std::path::Path,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    replica_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Replica)
      .replication_sidecar_path(local_sidecar)
      .replication_source_db_path(source_db_path)
      .replication_source_sidecar_path(source_sidecar),
  )
}

#[derive(Debug)]
struct CapturedHttpRequest {
  request_line: String,
  headers: HashMap<String, String>,
  body: Vec<u8>,
}

#[derive(Debug)]
struct CapturedGrpcRequest {
  authorization: Option<String>,
  resource_metrics_count: usize,
}

#[derive(Debug)]
struct TestGrpcMetricsService {
  tx: Mutex<Option<mpsc::Sender<CapturedGrpcRequest>>>,
}

#[tonic::async_trait]
impl OtelMetricsService for TestGrpcMetricsService {
  async fn export(
    &self,
    request: tonic::Request<OtelExportMetricsServiceRequest>,
  ) -> std::result::Result<tonic::Response<OtelExportMetricsServiceResponse>, tonic::Status> {
    let authorization = request
      .metadata()
      .get("authorization")
      .and_then(|value| value.to_str().ok())
      .map(ToOwned::to_owned);
    if let Some(sender) = self.tx.lock().expect("lock capture sender").take() {
      sender
        .send(CapturedGrpcRequest {
          authorization,
          resource_metrics_count: request.get_ref().resource_metrics.len(),
        })
        .expect("send grpc capture");
    }
    Ok(tonic::Response::new(OtelExportMetricsServiceResponse {
      partial_success: None,
    }))
  }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
  haystack
    .windows(needle.len())
    .position(|window| window == needle)
}

fn spawn_http_capture_server(
  status_code: u16,
  response_body: &str,
) -> (
  String,
  mpsc::Receiver<CapturedHttpRequest>,
  thread::JoinHandle<()>,
) {
  let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
  let address = listener.local_addr().expect("local addr");
  let endpoint = format!("http://{address}/v1/metrics");
  let response_body = response_body.to_string();
  let (tx, rx) = mpsc::channel::<CapturedHttpRequest>();

  let handle = thread::spawn(move || {
    let (mut stream, _) = listener.accept().expect("accept");
    stream
      .set_read_timeout(Some(Duration::from_secs(2)))
      .expect("set read timeout");

    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    let mut header_end: Option<usize> = None;
    let mut content_length = 0usize;

    loop {
      match stream.read(&mut chunk) {
        Ok(0) => break,
        Ok(read) => {
          buffer.extend_from_slice(&chunk[..read]);

          if header_end.is_none() {
            if let Some(position) = find_subsequence(&buffer, b"\r\n\r\n") {
              let end = position + 4;
              header_end = Some(end);
              let headers_text = String::from_utf8_lossy(&buffer[..end]);
              for line in headers_text.lines().skip(1) {
                let Some((name, value)) = line.split_once(':') else {
                  continue;
                };
                if name.eq_ignore_ascii_case("content-length") {
                  content_length = value.trim().parse::<usize>().unwrap_or(0);
                }
              }
            }
          }

          if let Some(end) = header_end {
            if buffer.len() >= end + content_length {
              break;
            }
          }
        }
        Err(error) => panic!("read request failed: {error}"),
      }
    }

    let end = header_end.expect("header terminator");
    let headers_text = String::from_utf8_lossy(&buffer[..end]);
    let mut lines = headers_text.lines();
    let request_line = lines.next().unwrap_or_default().to_string();
    let mut headers = HashMap::new();
    for line in lines {
      let Some((name, value)) = line.split_once(':') else {
        continue;
      };
      headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let body_end = (end + content_length).min(buffer.len());
    let body = buffer[end..body_end].to_vec();
    tx.send(CapturedHttpRequest {
      request_line,
      headers,
      body,
    })
    .expect("send captured request");

    let reason = if status_code == 200 { "OK" } else { "ERR" };
    let response = format!(
      "HTTP/1.1 {status_code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
      response_body.len(),
      response_body
    );
    stream
      .write_all(response.as_bytes())
      .expect("write response");
  });

  (endpoint, rx, handle)
}

fn spawn_grpc_capture_server() -> (
  String,
  mpsc::Receiver<CapturedGrpcRequest>,
  tokio::sync::oneshot::Sender<()>,
  thread::JoinHandle<()>,
) {
  let listener = TcpListener::bind("127.0.0.1:0").expect("bind grpc test server");
  let address = listener.local_addr().expect("grpc local addr");
  drop(listener);
  let endpoint = format!("http://{address}");
  let (tx, rx) = mpsc::channel::<CapturedGrpcRequest>();
  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

  let handle = thread::spawn(move || {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("create grpc runtime");
    runtime.block_on(async move {
      let service = TestGrpcMetricsService {
        tx: Mutex::new(Some(tx)),
      };
      tonic::transport::Server::builder()
        .add_service(OtelMetricsServiceServer::new(service))
        .serve_with_shutdown(address, async move {
          let _ = shutdown_rx.await;
        })
        .await
        .expect("serve grpc test endpoint");
    });
  });

  (endpoint, rx, shutdown_tx, handle)
}

#[test]
fn collect_metrics_exposes_primary_replication_fields() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("replication-metrics-primary.kitedb");
  let sidecar = dir.path().join("replication-metrics-primary.sidecar");

  let primary = open_primary(&db_path, &sidecar, 1, 2).expect("open primary");

  for i in 0..4 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("p-{i}")))
      .expect("create node");
    let _ = primary.commit_with_token().expect("commit").expect("token");
  }

  primary
    .primary_report_replica_progress("replica-a", 1, 2)
    .expect("report replica progress");

  let metrics = collect_metrics_single_file(&primary);
  let otel = collect_replication_metrics_otel_json_single_file(&primary);
  let otel_protobuf = collect_replication_metrics_otel_protobuf_single_file(&primary);
  let prometheus = collect_replication_metrics_prometheus_single_file(&primary);
  assert!(metrics.replication.enabled);
  assert_eq!(metrics.replication.role, "primary");
  assert!(metrics.replication.replica.is_none());

  let repl = metrics
    .replication
    .primary
    .as_ref()
    .expect("primary replication metrics");
  assert_eq!(repl.epoch, 1);
  assert_eq!(repl.replica_count, 1);
  assert_eq!(repl.stale_epoch_replica_count, 0);
  assert_eq!(repl.min_replica_applied_log_index, Some(2));
  assert_eq!(repl.max_replica_lag, repl.head_log_index.saturating_sub(2));
  assert!(repl.append_attempts >= repl.append_successes);
  assert_eq!(repl.append_failures, 0);
  assert!(repl.append_successes >= 4);
  assert!(repl.last_token.is_some());
  assert!(repl
    .sidecar_path
    .ends_with("replication-metrics-primary.sidecar"));
  assert!(prometheus.contains("# HELP kitedb_replication_enabled"));
  assert!(prometheus.contains("kitedb_replication_enabled{role=\"primary\"} 1"));
  assert!(prometheus.contains("kitedb_replication_primary_head_log_index"));
  assert!(prometheus.contains("kitedb_replication_primary_append_attempts_total"));
  assert!(otel.contains("\"kitedb.replication.enabled\""));
  assert!(otel.contains("\"kitedb.replication.primary.head_log_index\""));
  assert!(otel.contains("\"kitedb.replication.primary.append_attempts\""));
  let otel_json: serde_json::Value = serde_json::from_str(&otel).expect("parse otel json");
  assert!(otel_json["resourceMetrics"]
    .as_array()
    .map(|values| !values.is_empty())
    .unwrap_or(false));
  let otel_proto = OtelExportMetricsServiceRequest::decode(otel_protobuf.as_slice())
    .expect("decode otel protobuf request");
  assert_eq!(otel_proto.resource_metrics.len(), 1);
  let metric_names = otel_proto.resource_metrics[0]
    .scope_metrics
    .iter()
    .flat_map(|scope| scope.metrics.iter().map(|metric| metric.name.clone()))
    .collect::<Vec<_>>();
  assert!(metric_names
    .iter()
    .any(|name| name == "kitedb.replication.enabled"));
  assert!(metric_names
    .iter()
    .any(|name| name == "kitedb.replication.primary.head_log_index"));
  assert!(metric_names
    .iter()
    .any(|name| name == "kitedb.replication.primary.append_attempts"));

  close_single_file(primary).expect("close primary");
}

#[test]
fn collect_metrics_exposes_replica_reseed_error_state() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir
    .path()
    .join("replication-metrics-replica-primary.kitedb");
  let primary_sidecar = dir
    .path()
    .join("replication-metrics-replica-primary.sidecar");
  let replica_path = dir.path().join("replication-metrics-replica.kitedb");
  let replica_sidecar = dir.path().join("replication-metrics-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar, 1, 2).expect("open primary");

  primary.begin(false).expect("begin base");
  primary.create_node(Some("base")).expect("create base");
  primary
    .commit_with_token()
    .expect("commit base")
    .expect("token base");

  let replica = open_replica(
    &replica_path,
    &primary_path,
    &replica_sidecar,
    &primary_sidecar,
  )
  .expect("open replica");
  replica
    .replica_bootstrap_from_snapshot()
    .expect("bootstrap replica");

  for i in 0..5 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("r-{i}")))
      .expect("create");
    primary.commit_with_token().expect("commit").expect("token");
  }

  primary
    .primary_report_replica_progress("replica-r", 1, 1)
    .expect("report lagging replica");
  let _ = primary.primary_run_retention().expect("run retention");

  let err = replica
    .replica_catch_up_once(32)
    .expect_err("must need reseed");
  assert!(err.to_string().contains("reseed"));

  let metrics = collect_metrics_single_file(&replica);
  let otel = collect_replication_metrics_otel_json_single_file(&replica);
  let prometheus = render_replication_metrics_prometheus(&metrics);
  assert!(metrics.replication.enabled);
  assert_eq!(metrics.replication.role, "replica");
  assert!(metrics.replication.primary.is_none());

  let repl = metrics
    .replication
    .replica
    .as_ref()
    .expect("replica replication metrics");
  assert!(repl.needs_reseed);
  assert!(
    repl
      .last_error
      .as_deref()
      .unwrap_or_default()
      .contains("reseed"),
    "unexpected last_error: {:?}",
    repl.last_error
  );
  assert!(prometheus.contains("kitedb_replication_enabled{role=\"replica\"} 1"));
  assert!(prometheus.contains("kitedb_replication_replica_needs_reseed 1"));
  assert!(prometheus.contains("kitedb_replication_replica_last_error_present 1"));
  assert!(otel.contains("\"kitedb.replication.replica.needs_reseed\""));
  assert!(otel.contains("\"kitedb.replication.replica.last_error_present\""));

  close_single_file(replica).expect("close replica");
  close_single_file(primary).expect("close primary");
}

#[test]
fn replication_prometheus_export_reports_disabled_role() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("replication-metrics-disabled.kitedb");
  let db = open_single_file(&db_path, SingleFileOpenOptions::new()).expect("open db");

  let metrics = collect_metrics_single_file(&db);
  let otel = collect_replication_metrics_otel_json_single_file(&db);
  let prometheus = render_replication_metrics_prometheus(&metrics);
  assert!(!metrics.replication.enabled);
  assert_eq!(metrics.replication.role, "disabled");
  assert!(prometheus.contains("kitedb_replication_enabled{role=\"disabled\"} 0"));
  assert!(prometheus.contains("kitedb_replication_auth_enabled 0"));
  assert!(otel.contains("\"kitedb.replication.enabled\""));
  assert!(otel.contains("\"role\""));
  assert!(otel.contains("\"disabled\""));

  close_single_file(db).expect("close db");
}

#[test]
fn otlp_push_payload_validates_endpoint_and_timeout() {
  let endpoint_err = push_replication_metrics_otel_json_payload("{}", " ", 1000, None)
    .expect_err("empty endpoint must fail");
  assert!(endpoint_err.to_string().contains("endpoint"));

  let timeout_err =
    push_replication_metrics_otel_json_payload("{}", "http://127.0.0.1:1/v1/metrics", 0, None)
      .expect_err("zero timeout must fail");
  assert!(timeout_err.to_string().contains("timeout_ms"));
}

#[test]
fn otlp_push_payload_posts_json_and_auth_header() {
  let payload = "{\"resourceMetrics\":[]}";
  let (endpoint, captured_rx, handle) = spawn_http_capture_server(200, "ok");

  let result = push_replication_metrics_otel_json_payload(payload, &endpoint, 2_000, Some("token"))
    .expect("otlp push must succeed");
  assert_eq!(result.status_code, 200);
  assert_eq!(result.response_body, "ok");

  let captured = captured_rx
    .recv_timeout(Duration::from_secs(2))
    .expect("captured request");
  assert_eq!(captured.request_line, "POST /v1/metrics HTTP/1.1");
  assert_eq!(
    captured.headers.get("content-type").map(String::as_str),
    Some("application/json")
  );
  assert_eq!(
    captured.headers.get("authorization").map(String::as_str),
    Some("Bearer token")
  );
  assert_eq!(String::from_utf8_lossy(&captured.body), payload);

  handle.join().expect("server thread");
}

#[test]
fn otlp_push_protobuf_payload_posts_binary_and_auth_header() {
  let payload = vec![0x0a, 0x03, 0x66, 0x6f, 0x6f];
  let (endpoint, captured_rx, handle) = spawn_http_capture_server(200, "ok");

  let result =
    push_replication_metrics_otel_protobuf_payload(&payload, &endpoint, 2_000, Some("token"))
      .expect("otlp protobuf push must succeed");
  assert_eq!(result.status_code, 200);
  assert_eq!(result.response_body, "ok");

  let captured = captured_rx
    .recv_timeout(Duration::from_secs(2))
    .expect("captured request");
  assert_eq!(captured.request_line, "POST /v1/metrics HTTP/1.1");
  assert_eq!(
    captured.headers.get("content-type").map(String::as_str),
    Some("application/x-protobuf")
  );
  assert_eq!(
    captured.headers.get("authorization").map(String::as_str),
    Some("Bearer token")
  );
  assert_eq!(captured.body, payload);

  handle.join().expect("server thread");
}

#[test]
fn otlp_push_payload_returns_error_on_non_success_status() {
  let payload = "{\"resourceMetrics\":[]}";
  let (endpoint, _captured_rx, handle) = spawn_http_capture_server(401, "denied");

  let error = push_replication_metrics_otel_json_payload(payload, &endpoint, 2_000, None)
    .expect_err("non-2xx must fail");
  let message = error.to_string();
  assert!(
    message.contains("status 401"),
    "unexpected error: {message}"
  );
  assert!(message.contains("denied"), "unexpected error: {message}");

  handle.join().expect("server thread");
}

#[test]
fn otlp_push_payload_rejects_https_only_http_endpoint() {
  let options = OtlpHttpPushOptions {
    timeout_ms: 2_000,
    bearer_token: None,
    tls: OtlpHttpTlsOptions {
      https_only: true,
      ..OtlpHttpTlsOptions::default()
    },
  };
  let error = push_replication_metrics_otel_json_payload_with_options(
    "{}",
    "http://127.0.0.1:4318/v1/metrics",
    &options,
  )
  .expect_err("https_only should reject http endpoint");
  assert!(error.to_string().contains("https"));
}

#[test]
fn otlp_push_payload_rejects_partial_mtls_paths() {
  let options = OtlpHttpPushOptions {
    timeout_ms: 2_000,
    bearer_token: None,
    tls: OtlpHttpTlsOptions {
      client_cert_pem_path: Some("/tmp/client.crt".to_string()),
      client_key_pem_path: None,
      ..OtlpHttpTlsOptions::default()
    },
  };
  let error = push_replication_metrics_otel_json_payload_with_options(
    "{}",
    "https://127.0.0.1:4318/v1/metrics",
    &options,
  )
  .expect_err("partial mTLS path configuration should fail");
  assert!(error.to_string().contains("client_cert_pem_path"));
  assert!(error.to_string().contains("client_key_pem_path"));
}

#[test]
fn otlp_push_grpc_payload_posts_request_and_auth_header() {
  let payload = OtelExportMetricsServiceRequest {
    resource_metrics: Vec::new(),
  }
  .encode_to_vec();
  let (endpoint, captured_rx, shutdown_tx, handle) = spawn_grpc_capture_server();
  thread::sleep(Duration::from_millis(50));

  let result =
    push_replication_metrics_otel_grpc_payload(&payload, &endpoint, 2_000, Some("token"))
      .expect("otlp grpc push must succeed");
  assert_eq!(result.status_code, 200);

  let captured = captured_rx
    .recv_timeout(Duration::from_secs(2))
    .expect("captured grpc request");
  assert_eq!(captured.authorization.as_deref(), Some("Bearer token"));
  assert_eq!(captured.resource_metrics_count, 0);

  let _ = shutdown_tx.send(());
  handle.join().expect("grpc server thread");
}

#[test]
fn otlp_push_grpc_payload_rejects_invalid_protobuf() {
  let error = push_replication_metrics_otel_grpc_payload(
    &[0xff, 0x00, 0x12],
    "http://127.0.0.1:4317",
    2_000,
    None,
  )
  .expect_err("invalid protobuf payload must fail");
  assert!(error.to_string().contains("Invalid OTLP protobuf payload"));
}
