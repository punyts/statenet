# Changelog

## Unreleased

- **The mesh: links between nodes, and relay processes.** `startMeshServer` is a node's side of the links below it: one
  WebSocket per node, opened with a certificate Magnetic's authority signed (`issueNodeCertificate`) and a proof of the
  node's key each way, then frames of `subscribe`, `renew`, `changes`, `durable`, `write`, `revoked`, `credit` and
  `directory` messages. A subscription's changes cost a credit each; without credit they are folded, and past `maxHeld`
  the node below is told to catch up instead. `createMeshUpstream` is the side looking up: it learns the directory from
  its parent, reconnects to the next parent when a link goes silent, and subscribes again from each subscription's
  revision. `startRelayNode` (and the `statenet-relay` command) runs a relay with no database, serving relays below over
  the mesh and browsers over the sync protocol (`startClientServer`). Tests: `mesh.test.ts`.
- **Emit relay.** An `emit` from an app server (a client without `Origin`) is relayed to every other client of the
  namespace, an `emit` from a web page to the app servers only; nothing of it is stored. `@punyts/stateui-mcp` sends
  its view messages (documents, commands) this way and its pages their reports, app events and file requests.
- **Leaving.** When a client of a namespace disconnects, the others get the emit `statenet.left` with its `{ clientId }`
  (`LEFT_EMIT`).
- README: how stateui-mcp uses the statenet; the stale `$.sealed` relay description is gone.

## 0.1.0

- First version: `startStatenet` and the `statenet` CLI (`--port`, `--host`, `--data-dir`, `--log-length`,
  `--allow-origin`) run one `@punyts/state-sync` authority per namespace, with no server logic, behind a WebSocket at
  `/sync`. A namespace starts empty and creates missing parents.
- `createFileNamespaceStorage`: one folder per namespace with `namespace.json`, written atomically, one writer at a
  time per namespace; temporary files a crash left are removed on load.
- Listens on loopback only; refuses a non-loopback `Host` and browser origins that are not allowed.
- README: how an app server relays to the statenet (`$.server` as `$.sealed`) and how Persisted pages are admitted.
- Logs through the `@punyts/core` reporter, never change values.
