import test from 'ava'

import {
  authorizeReplicationAdminRequest,
  createReplicationAdminAuthorizer,
  isReplicationAdminAuthorized,
  type ReplicationAdminAuthRequest,
} from '../ts/replication_transport'

type RequestLike = ReplicationAdminAuthRequest & {
  tlsAuthorized?: boolean
}

function request(headers: Record<string, string | undefined> = {}): RequestLike {
  return { headers }
}

test('replication admin auth none mode always allows', (t) => {
  t.true(isReplicationAdminAuthorized(request(), { mode: 'none' }))
  t.notThrows(() => authorizeReplicationAdminRequest(request(), { mode: 'none' }))
})

test('replication admin auth token mode requires bearer token', (t) => {
  const cfg = { mode: 'token', token: 'abc123' } as const
  t.true(isReplicationAdminAuthorized(request({ authorization: 'Bearer abc123' }), cfg))
  t.false(isReplicationAdminAuthorized(request({ authorization: 'Bearer no' }), cfg))
  t.false(isReplicationAdminAuthorized(request({}), cfg))
})

test('replication admin auth mtls mode supports header + subject regex', (t) => {
  const cfg = {
    mode: 'mtls',
    mtlsHeader: 'x-client-cert',
    mtlsSubjectRegex: /^CN=replication-admin,/,
  } as const
  t.true(isReplicationAdminAuthorized(request({ 'x-client-cert': 'CN=replication-admin,O=RayDB' }), cfg))
  t.false(isReplicationAdminAuthorized(request({ 'x-client-cert': 'CN=viewer,O=RayDB' }), cfg))
})

test('replication admin auth token_or_mtls accepts either', (t) => {
  const cfg = {
    mode: 'token_or_mtls',
    token: 'abc123',
    mtlsHeader: 'x-client-cert',
  } as const
  t.true(isReplicationAdminAuthorized(request({ authorization: 'Bearer abc123' }), cfg))
  t.true(isReplicationAdminAuthorized(request({ 'x-client-cert': 'CN=replication-admin,O=RayDB' }), cfg))
  t.false(isReplicationAdminAuthorized(request({}), cfg))
})

test('replication admin auth token_and_mtls requires both', (t) => {
  const cfg = {
    mode: 'token_and_mtls',
    token: 'abc123',
    mtlsHeader: 'x-client-cert',
  } as const
  t.false(isReplicationAdminAuthorized(request({ authorization: 'Bearer abc123' }), cfg))
  t.false(isReplicationAdminAuthorized(request({ 'x-client-cert': 'CN=replication-admin,O=RayDB' }), cfg))
  t.true(
    isReplicationAdminAuthorized(
      request({
        authorization: 'Bearer abc123',
        'x-client-cert': 'CN=replication-admin,O=RayDB',
      }),
      cfg,
    ),
  )
})

test('replication admin auth supports custom mtls matcher hook', (t) => {
  const cfg = {
    mode: 'mtls',
    mtlsMatcher: (req: RequestLike) => req.tlsAuthorized === true,
  }
  t.true(isReplicationAdminAuthorized({ headers: {}, tlsAuthorized: true }, cfg))
  t.false(isReplicationAdminAuthorized({ headers: {}, tlsAuthorized: false }, cfg))
})

test('replication admin auth helper throws unauthorized and invalid config', (t) => {
  const requireAdmin = createReplicationAdminAuthorizer({
    mode: 'token',
    token: 'abc123',
  })
  const error = t.throws(() => requireAdmin(request({ authorization: 'Bearer wrong' })))
  t.truthy(error)
  t.true(String(error?.message).includes('not satisfied'))

  const invalid = t.throws(() =>
    createReplicationAdminAuthorizer({
      mode: 'token',
      token: '   ',
    }),
  )
  t.truthy(invalid)
  t.true(String(invalid?.message).includes('requires a non-empty token'))
})
