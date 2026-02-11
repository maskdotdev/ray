import test from 'ava'
import fs from 'node:fs'

const source = fs.readFileSync(new URL('../ts/index.ts', import.meta.url), 'utf8')

test('TS wrapper bridges camel methods to snake_case N-API methods', (t) => {
  const bridges: Array<[camel: string, snake: string]> = [
    ['getRef', 'get_ref'],
    ['getId', 'get_id'],
    ['getById', 'get_by_id'],
    ['getByIds', 'get_by_ids'],
    ['getProp', 'get_prop'],
    ['getEdgeProp', 'get_edge_prop'],
    ['getEdgeProps', 'get_edge_props'],
  ]

  for (const [camel, snake] of bridges) {
    t.true(source.includes(`super.${snake}(`), `expected ${camel} to call super.${snake}()`)
    t.false(source.includes(`super.${camel}(`), `expected ${camel} not to call super.${camel}()`)
  }
})
