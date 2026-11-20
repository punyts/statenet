import type { Change } from '@punyts/state-sync';
import type { Access, GrantKey, RelayBatch, RelayWelcome, StoredNamespace, WriteOutcome } from '@punyts/state-sync/authority';
import type { NodeRole } from './certificates.js';

/** The version of the mesh protocol, named in `link-hello` */
export const MESH_PROTOCOL = 2;

/**
 * What a parent tells a node below it about the statenet (docs/design/statenet-relays.md, "The directory"), so a relay
 * learns what it needs without database credentials: the nodes, who owns which namespace, and the public keys that
 * sign grants
 */
export interface DirectoryView {
    readonly nodes: ReadonlyArray<{ readonly id: string; readonly role: NodeRole; readonly region: string; readonly endpoint: string; /** The node's key as it registered it; a server that asks for listed nodes checks a certificate's key against it */ readonly publicKey?: string }>;
    readonly owners: Readonly<Record<string, { readonly node: string; readonly epoch: number }>>;
    readonly keys: Readonly<Record<string, GrantKey>>;
}

/** An aggregate's answer to a save: it landed (up to `revision`), a newer epoch holds the namespace, or it failed */
export type PersistOutcome = { result: 'saved'; revision: number } | { result: 'fenced'; epoch: number; current: number } | { result: 'failed'; reason: string };

/** An aggregate's answer to a load: the stored namespace (none when nothing is stored), or why not */
export type LoadOutcome = { ok: true; stored?: StoredNamespace } | { ok: false; reason: string };

/**
 * The handshake: each end shows its certificate and its share of the link's key, and signs the other's challenge with
 * both shares (`transcript`). These go in the clear; everything after `link-proof` is sealed (`sealOf`), and the
 * first sealed frame, from the server, starts with the directory
 */
export type LinkHello = { type: 'link-hello'; nodeId: string; role: NodeRole; protocol: number; region: string; certificate: string; nonce: string; share: string };
export type LinkWelcome = { type: 'link-welcome'; nodeId: string; certificate: string; nonce: string; proof: string; share: string };
export type LinkProof = { type: 'link-proof'; proof: string };
export type LinkRefused = { type: 'link-refused'; reason: string };

/** The messages a frame carries, each naming its subscription (`sid`) or write (`wid`) on the link */
export type MeshMessage =
    | { type: 'subscribe'; sid: number; address: string; prefix: string; revision?: number; access?: Access }
    | { type: 'renew'; sid: number }
    | { type: 'unsubscribe'; sid: number }
    | { type: 'welcome'; sid: number; welcome: RelayWelcome }
    | { type: 'changes'; sid: number; batch: RelayBatch }
    | { type: 'durable'; sid: number; revision: number }
    | { type: 'closed'; sid: number; reason: string }
    /** The parent dropped what it held back for a slow subscription: subscribe again from your revision */
    | { type: 'resync'; sid: number }
    | { type: 'revoked'; grantId: string; expires: number }
    | { type: 'write'; wid: number; address: string; change: Change; access?: Access }
    | { type: 'written'; wid: number; outcome: WriteOutcome }
    | { type: 'credit'; amount: number }
    | { type: 'directory'; directory: DirectoryView }
    /**
     * A node's namespace on its way to the database (docs/design/statenet-network.md, "Aggregates write to the
     * database"): relays pass it up to an aggregate, naming the `node` that orders it, and pass the answer back
     */
    | { type: 'persist'; pid: number; address: string; epoch: number; stored: StoredNamespace; node?: string }
    | { type: 'persisted'; pid: number; outcome: PersistOutcome }
    | { type: 'load'; lid: number; address: string; node?: string }
    | { type: 'loaded'; lid: number; outcome: LoadOutcome }
    /** A node asks for a certificate's next one (its own, or one a node below it asked for); none when it is refused */
    | { type: 'certify'; rid: number; certificate: string }
    | { type: 'certified'; rid: number; certificate?: string }
    /**
     * A session of a namespace the node at the other end owns, tunnelled for a view this node serves: `open` starts it,
     * `say` carries each sync message either way as its text, `shut` ends it from either end
     */
    | { type: 'open'; tid: number; address: string; access?: Access }
    | { type: 'say'; tid: number; text: string }
    | { type: 'shut'; tid: number; reason: string }
    /** A claim (docs/design/statenet-relays.md, "Ownership moves by a claim"): the owner answers with its handoff, or none */
    | { type: 'claim'; cid: number; address: string }
    | { type: 'handoff'; cid: number; handoff?: { revision: number; epoch: number; queued: Array<{ qid: number; change: Change }> } }
    /** The claimant's outcome of a write that was queued in the handoff, for the node that is waiting on it */
    | { type: 'handed'; cid: number; qid: number; outcome: WriteOutcome }
    /** The owner asks the node at the other end to claim a namespace (it sends most of the writes), and hears whether it did */
    | { type: 'take'; cid: number; address: string }
    | { type: 'taken'; cid: number; ok: boolean };

/** Many messages from one moment, of many namespaces, in one WebSocket message */
export type MeshFrame = { type: 'frame'; messages: MeshMessage[] };

export type LinkMessage = LinkHello | LinkWelcome | LinkProof | LinkRefused | MeshFrame | { type: 'ping' } | { type: 'pong' };

/** Parses one message of a link (the WebSocket's in the handshake, the opened text after it), or undefined when it is not one */
export function parseLinkMessage(data: unknown): LinkMessage | undefined {
    try {
        const text = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : Array.isArray(data) ? Buffer.concat(data as Buffer[]).toString('utf8') : Buffer.from(data as ArrayBuffer).toString('utf8');
        const message = JSON.parse(text) as { type?: unknown };
        return typeof message === 'object' && message !== null && typeof message.type === 'string' ? (message as LinkMessage) : undefined;
    } catch {
        return undefined;
    }
}
