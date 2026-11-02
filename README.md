# @punyts/statenet

A Node service that persists [`@punyts/state-sync`](https://github.com/punyts/state-sync) namespaces and speaks the
sync protocol over a WebSocket. Browsers (Persisted mode) and app servers (Full mode) connect to it as sync clients;
it keeps each namespace's state, revision and recent changes, applies changes in arrival order and pushes them to the
other clients of the namespace. It has no logic of its own beyond validation, revisions and acks.

## Run it

```bash
npx statenet --data-dir ./data
```

| Option | Environment | Default | What |
|---|---|---|---|
| `--data-dir <dir>` | `STATENET_DATA_DIR` | `./data` | Where the namespaces are stored, one folder each |
| `--port <n>` | `STATENET_PORT` | `4200` | Port to listen on |
| `--host <address>` | `STATENET_HOST` | `127.0.0.1` | Loopback address to bind; anything else is refused |
| `--log-length <n>` | `STATENET_LOG_LENGTH` | `1000` | Recent changes each namespace keeps for catching clients up |
| `--allow-origin <url>` | `STATENET_ALLOW_ORIGINS` (comma-separated) | none | A browser origin that may connect; repeat for more |

Clients connect to `ws://<host>:<port>/sync`. `GET /health` answers `{ "ok": true }`.

## Use it from code

```ts
import { startStatenet } from "@punyts/statenet";

const statenet = await startStatenet({ dataDir: "./data", port: 4200, allowedOrigins: ["http://127.0.0.1:4100"] });
console.log(statenet.url); // ws://127.0.0.1:4200/sync
await statenet.close();
```

`startStatenet` takes `host`, `port`, `dataDir`, `storage` (any `NamespaceStorage` from
`@punyts/state-sync/authority`, instead of the files), `logLength` and `allowedOrigins`. It returns `{ url, port,
namespaces, close }`; `close` closes the connections, waits for the pending saves and stops listening.

## How it works

- **Namespaces.** A connection names its namespace with its first message, the protocol's `hello`; any other first
  message is answered with an `error` and the socket is closed (1008). The namespace is loaded from storage the first
  time it is asked for, and one authority serves every client of it.
- **Shape.** A namespace starts empty and the statenet creates missing parents of a change, since it does not know the
  app's shape.
- **Storage.** `createFileNamespaceStorage({ dataDir })` keeps one folder per namespace, named by the namespace, with
  `namespace.json` (state, revision, change log and applied change ids). A save writes a temporary file and renames it
  over the last one, so a crash leaves the last good file; the saves of one namespace run one at a time, in order, and
  temporary files a crash left are removed when the namespace loads. The storage is a port: a database adapter can take
  its place.
- **Logging.** Through the `@punyts/core` reporter (categories `statenet` and `state-sync`): namespaces, revisions and
  errors, never change values.

## Security

- **Loopback only.** The statenet has no authentication, so it refuses to start on an address that is not loopback
  (`127.0.0.0/8`, `::1`, `localhost`).
- **Host check.** A request whose `Host` is not a loopback name is refused (403), so a page on another name that
  resolves to 127.0.0.1 cannot reach it.
- **Origins.** A browser always sends `Origin` with a WebSocket. A connection with an `Origin` is accepted only when
  the origin is in `allowedOrigins`; clients without one (Node, app servers) are accepted. With no origins allowed, no
  web page can connect.
- **`$.server`.** An app server relays `$.server` encrypted; the statenet stores the ciphertext and cannot read it.
