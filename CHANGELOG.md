# Changelog

## 0.1.0

- First version: `startStatenet` and the `statenet` CLI (`--port`, `--host`, `--data-dir`, `--log-length`,
  `--allow-origin`) run one `@punyts/state-sync` authority per namespace, with no server logic, behind a WebSocket at
  `/sync`. A namespace starts empty and creates missing parents.
- `createFileNamespaceStorage`: one folder per namespace with `namespace.json`, written atomically, one writer at a
  time per namespace; temporary files a crash left are removed on load.
- Listens on loopback only; refuses a non-loopback `Host` and browser origins that are not allowed.
- README: how an app server relays to the statenet (`$.server` as `$.sealed`) and how Persisted pages are admitted.
- Logs through the `@punyts/core` reporter, never change values.
