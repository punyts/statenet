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

Clients connect to `ws://<host>:<port>/sync`. `GET /health` answers `{ "ok": true }`. `npm start` runs `node dist/cli.js`.

### Reporting
Both bins write reports to stderr, one line each, `[statenet] <category> <message>` (`[statenet-relay]` for the
relay). They turn on `statenet.*`, `state-sync.*`, `info`, `warning` and `error`, and the categories
`PUNYTS_REPORT` lists: comma-separated, trimmed, empty entries ignored, a family as `<library>.*`
(`PUNYTS_REPORT=state.set,stateui-app.*`). Reports name namespaces, addresses, node ids, paths, revisions, counts and
durations, never a value.

| Category | Reports |
|---|---|
| `statenet.service` | namespace loads with ms, clients joining and leaving, refused connections and upgrades, emits relayed |
| `statenet.mesh` | mesh servers listening, links opened and closed, links up, reconnects with their delay, nodes dropped from the directory |
| `statenet.mesh.serving` | subscriptions served, slow nodes told to catch up, sessions, handoffs, saves and writes passed on with their outcome |
| `statenet.owner` | an owner node's start and stop, peer links, following a namespace's owner, sessions placed |
| `statenet.relay` | a relay's start and stop, the directory it learned, links to other groups' relays |
| `statenet.relay.client` | browsers' subscriptions and the outcome of their changes |
| `statenet.aggregate` | saves with ms, folded saves, fenced epochs |
| `statenet.certificate` | certificate renewals asked, given and passed on |

Refused links and saves and unanswered writes are warnings, a failed save an error; listening and stopping are info.
`state-sync.*` is listed in state-sync's README.

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
- **Emit relay.** The protocol's `emit` (a named event outside the state) is relayed between the clients of a
  namespace and never stored: an emit from an app server (a client without `Origin`) goes to every other client of the
  namespace, an emit from a web page to the app servers only, so one page cannot speak for the server to another. A
  client that is not connected misses it.
- **Leaving.** When a client of a namespace disconnects, the others get the emit `statenet.left` (`LEFT_EMIT`) with
  `{ clientId }`, the id it gave in its `hello`, so an app server can forget a page that closed.
- **Storage.** `createFileNamespaceStorage({ dataDir })` keeps one folder per namespace, named by the namespace, with
  `namespace.json` (state, revision, change log and applied change ids). A save writes a temporary file and renames it
  over the last one, so a crash leaves the last good file; the saves of one namespace run one at a time, in order, and
  temporary files a crash left are removed when the namespace loads. The storage is a port: a database adapter can take
  its place.
- **Logging.** Through the `@punyts/core` reporter (categories `statenet` and `state-sync`): namespaces, revisions and
  errors, never change values.

## With an app server
An app server connects as a client without an `Origin` and keeps its own authority with the server logic.
`@punyts/stateui-mcp` does this: it serves no HTTP and writes every app's live state here, and its view pages (static
files from Vite or a CDN) connect here directly, so start the statenet with `--allow-origin` set to the origin of
those pages (`npm run dev` in stateui-mcp does). Per open app it bridges the namespace and its own authority both ways:
the changes its authority makes (logic, tools) are written here, and a page's change, pushed to it like to any client,
goes to its authority, which may put the value back. The document, commands, reports and files go over the emit relay.
It keeps a directory of the apps in the namespace `stateui.apps`. `$.server` never leaves the app server.

## The mesh

Nodes of a larger statenet link to each other (`docs/design/statenet-relays.md`, "The mesh protocol"): an owner node
serves its namespaces down, and relays cache what is subscribed through them and serve browsers at the edge.

- Every node has its own Ed25519 key and a certificate from Magnetic's authority (`issueNodeCertificate`); a link opens
  only between nodes that show one and prove they hold their key, and only between roles that link (`mayLink`: a node
  to a relay or a node beside it, a relay to an aggregate or a relay). A server can also ask that a node be in the
  directory (`requireListed`).
- Everything on a link after its handshake is sealed with a key the two ends agree in it, with or without TLS
  underneath (`docs/design/statenet-network.md`).
- A node links up to a relay (`startOwnerMesh` with `relays`), and a link carries asks both ways, so the relay
  reaches the nodes below it. Nodes are in groups (`group`, in their certificates): those of one group link to each
  other directly, and what is for a node of another group goes through the relays, which find it by the directory:
  the node's `parent` (the relay its link reached, `onUplinked`) and that relay's `endpoint`. An aggregate with
  `relays` (`RelayListing`) lists each relay linked to it from its certificate (`endpoint` and `group` are claims) while
  its link is up, so the directory needs no row written by hand (`docs/design/statenet-relay-directory.md`). A relay
  passes up the relays linked below it, and the aggregate lists each under the relay it is linked to, so a relay
  under a relay is routed to as any other (`docs/design/statenet-relay-chains.md`).
- A write goes to the namespace's owner node with the writer's access, so the owner's authority can check it; one from
  someone who may only request the path comes back `requested` and waits there for a decision
  (`docs/design/statenet-requests.md`). A decider anywhere decides over a session tunnelled to the owner.
- An aggregate is the only writer of state: `startAggregateNode` (or `createAggregate` in the node's own process) saves
  what nodes send it, and `createAggregateStorage` is the storage a node uses to save through one. With no database,
  `createFileEpochStorage` keeps the namespaces in files.
- A node asks its parent for its next certificate before the old one ends; `statenet-relay` writes it over its file.
- `startMeshServer` serves the links below a node; `createMeshUpstream` is a node's link up, which reconnects to another
  parent and catches up from where it was. Credits per link keep a slow node from growing its parent's memory.
- `statenet-relay` runs a relay process: it reaches no database and learns the owners, the nodes and the keys that sign
  grants from its parent.

```bash
statenet-relay --node-id relay-eu --parents wss://core.example/mesh --node-key key.json --certificate relay-eu.cert --authority-key <base64url>
```

## Security

- **Loopback only.** The statenet has no authentication, so it refuses to start on an address that is not loopback
  (`127.0.0.0/8`, `::1`, `localhost`).
- **Host check.** A request whose `Host` is not a loopback name is refused (403), so a page on another name that
  resolves to 127.0.0.1 cannot reach it.
- **Origins.** A browser always sends `Origin` with a WebSocket. A connection with an `Origin` is accepted only when
  the origin is in `allowedOrigins`; clients without one (Node, app servers) are accepted. With no origins allowed, no
  web page can connect.
- **`$.server`.** The statenet, like every authority, refuses client changes to `$.server`; an app server keeps it to itself.
- **Emits.** A web page's emits reach the app servers of its namespace only.
