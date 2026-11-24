import { decodeCbor, DEFAULT_SYNC_LIMITS, encodeCbor } from '@punyts/state-sync';
import type { CborValue, Change } from '@punyts/state-sync';
import type { Access, GrantKey, RelayBatch, RelayWelcome, StoredNamespace, WriteOutcome } from '@punyts/state-sync/authority';
import type { NodeRole } from './certificates.js';

/** The version of the mesh protocol, named in `link-hello` */
export const MESH_PROTOCOL = 5;

/** The longest WebSocket message a link carries, and so the longest sealed frame */
export const MAX_LINK_MESSAGE_BYTES = 16 * 1024 * 1024;

/**
 * What a parent tells a node below it about the statenet (docs/design/statenet-relays.md, "The directory"), so a relay
 * learns what it needs without database credentials: the nodes, who owns which namespace, and the public keys that
 * sign grants
 */
export interface DirectoryView {
    readonly nodes: ReadonlyArray<{ readonly id: string; readonly role: NodeRole; readonly region: string; readonly endpoint: string; /** A node's group, and the relay it is linked to: how a relay finds the way to a node of another group */ readonly group?: string; readonly parent?: string; /** The node's key as it registered it; a server that asks for listed nodes checks a certificate's key against it */ readonly publicKey?: string }>;
    readonly owners: Readonly<Record<string, { readonly node: string; readonly epoch: number }>>;
    readonly keys: Readonly<Record<string, GrantKey>>;
}

/** An aggregate's answer to a save: it landed (up to `revision`), a newer epoch holds the namespace, or it failed */
export type PersistOutcome = { result: 'saved'; revision: number } | { result: 'fenced'; epoch: number; current: number } | { result: 'failed'; reason: string };

/** An aggregate's answer to a removal: the namespace is gone, a newer epoch holds it, or it failed */
export type RemoveOutcome = { result: 'removed' } | { result: 'fenced'; epoch: number; current: number } | { result: 'failed'; reason: string };

/** An aggregate's answer to a load: the stored namespace (none when nothing is stored), or why not */
export type LoadOutcome = { ok: true; stored?: StoredNamespace } | { ok: false; reason: string };

/**
 * The handshake: each end shows its certificate and its share of the link's key, and signs the other's challenge with
 * both shares (`transcript`). These go in the clear; everything after `link-proof` is sealed (`sealOf`), and the
 * first sealed frame, from the server, starts with the directory
 */
/** `routing`: a relay opens the link to reach the nodes below the other end, not as its child (docs/design/statenet-relay-chains.md) */
export type LinkHello = { type: 'link-hello'; nodeId: string; role: NodeRole; protocol: number; region: string; certificate: string; nonce: string; share: string; routing?: boolean };
export type LinkWelcome = { type: 'link-welcome'; nodeId: string; certificate: string; nonce: string; proof: string; share: string };
export type LinkProof = { type: 'link-proof'; proof: string };
export type LinkRefused = { type: 'link-refused'; reason: string };

/**
 * The messages a frame carries, each naming its subscription (`sid`) or write (`wid`) on the link. An ask may name
 * the node it is for (`to`): a relay passes it on to that node, below it or below another relay
 * (docs/design/statenet-network.md, "Groups"), and names the node it came from (`from`) where the owner needs it
 */
export type MeshMessage =
    /** What the end that did not open the link asks of the other, and the answers to it: the same messages, the other way */
    | { type: 'rev'; message: MeshMessage }
    | { type: 'subscribe'; sid: number; address: string; prefix: string; revision?: number; access?: Access; to?: string }
    | { type: 'renew'; sid: number }
    | { type: 'unsubscribe'; sid: number }
    | { type: 'welcome'; sid: number; welcome: RelayWelcome }
    | { type: 'changes'; sid: number; batch: RelayBatch }
    | { type: 'durable'; sid: number; revision: number }
    | { type: 'closed'; sid: number; reason: string }
    /** The parent dropped what it held back for a slow subscription: subscribe again from your revision */
    | { type: 'resync'; sid: number }
    | { type: 'revoked'; entry: string; expires: number }
    | { type: 'write'; wid: number; address: string; change: Change; access?: Access; to?: string; from?: string }
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
    /** A node has its namespace removed whole (a reset, a deleted app), under the epoch it holds it under */
    | { type: 'remove'; lid: number; address: string; epoch: number; node?: string }
    | { type: 'removed'; lid: number; outcome: RemoveOutcome }
    /** A node asks for a certificate's next one (its own, or one a node below it asked for); none when it is refused */
    | { type: 'certify'; rid: number; certificate: string }
    | { type: 'certified'; rid: number; certificate?: string }
    /**
     * A session of a namespace the node at the other end owns, tunnelled for a view this node serves: `open` starts it,
     * `say` carries each sync message either way as the sync wire wrote it (`hello` and `error` as text, the rest as
     * bytes), `shut` ends it from either end
     */
    | { type: 'open'; tid: number; address: string; access?: Access; to?: string }
    | { type: 'say'; tid: number; data: string | Uint8Array }
    | { type: 'shut'; tid: number; reason: string }
    /** A claim (docs/design/statenet-relays.md, "Ownership moves by a claim"): the owner answers with its handoff, or none */
    | { type: 'claim'; cid: number; address: string; to?: string; from?: string }
    | { type: 'handoff'; cid: number; handoff?: { revision: number; epoch: number; queued: Array<{ qid: number; change: Change; access?: Access }> } }
    /** The claimant's outcome of a write that was queued in the handoff, for the node that is waiting on it */
    | { type: 'handed'; cid: number; qid: number; outcome: WriteOutcome }
    /** The owner asks the node at the other end to claim a namespace (it sends most of the writes), and hears whether it did */
    | { type: 'take'; cid: number; address: string; to?: string }
    | { type: 'taken'; cid: number; ok: boolean }
    /**
     * A relay linked below a relay, passed up link by link to the aggregate (docs/design/statenet-relay-chains.md):
     * `via` is the relay it is linked to, `certificate` the one it proved there. No answer
     */
    | { type: 'relay-linked'; certificate: string; via: string }
    /** The last link of relay `id` to relay `via` closed: it and every relay under it are unlisted */
    | { type: 'relay-unlinked'; id: string; via: string };

/** Many messages from one moment, of many namespaces, in one WebSocket message */
export type MeshFrame = { type: 'frame'; messages: MeshMessage[] };

export type LinkMessage = LinkHello | LinkWelcome | LinkProof | LinkRefused | MeshFrame | { type: 'ping' } | { type: 'pong' };

/** Parses one message of the handshake, JSON text in the clear, or undefined when it is not one */
export function parseLinkMessage(data: unknown): LinkMessage | undefined {
    try {
        const text = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : Array.isArray(data) ? Buffer.concat(data as Buffer[]).toString('utf8') : Buffer.from(data as ArrayBuffer).toString('utf8');
        const message = JSON.parse(text) as { type?: unknown };
        return typeof message === 'object' && message !== null && typeof message.type === 'string' ? (message as LinkMessage) : undefined;
    } catch {
        return undefined;
    }
}

/**
 * Mesh protocol 5 (docs/design/statenet-wire-format.md, W3): past the handshake a link message is CBOR, sealed. A
 * message's members that are undefined are left out, as JSON leaves them out, and its values nest at most 16 levels
 * deeper than a sync message's
 */
const CBOR_DEPTH = DEFAULT_SYNC_LIMITS.maxDepth + 16;
const CBOR_LIMITS = { maxBytes: MAX_LINK_MESSAGE_BYTES, maxDepth: CBOR_DEPTH, maxItems: MAX_LINK_MESSAGE_BYTES, maxStringBytes: MAX_LINK_MESSAGE_BYTES };

const encode = (message: LinkMessage | MeshMessage): Uint8Array => encodeCbor(message as unknown as CborValue, { bytes: true, omitUndefined: true, maxDepth: CBOR_DEPTH });

/**
 * The bytes of a message of an open link, to seal. A frame leaves out a message that cannot be encoded (a value JSON
 * would not hold, or one nested too deep) and names its type to `dropped`, so one bad value does not end the link
 */
export function encodeLinkMessage(message: LinkMessage, dropped?: (type: MeshMessage['type']) => void): Uint8Array {
    try {
        return encode(message);
    } catch (error) {
        if (message.type !== 'frame') throw error;
        const kept = message.messages.filter((each) => {
            try {
                encode(each);
                return true;
            } catch {
                dropped?.(each.type);
                return false;
            }
        });
        return encode({ type: 'frame', messages: kept });
    }
}

/** Whether a decoded value holds a byte string anywhere */
const holdsBytes = (value: CborValue): boolean => {
    if (value instanceof Uint8Array) return true;
    if (value === null || typeof value !== 'object') return false;
    if (Array.isArray(value)) return value.some(holdsBytes);
    for (const key in value) if (holdsBytes(value[key] as CborValue)) return true;
    return false;
};

/** Whether a decoded mesh message holds bytes only where a `say` carries them: its data */
const bytesInPlace = (message: CborValue): boolean => {
    if (message === null || typeof message !== 'object' || Array.isArray(message) || message instanceof Uint8Array) return !holdsBytes(message);
    if (message.type === 'say') {
        const { data, ...rest } = message;
        return (typeof data === 'string' || data instanceof Uint8Array) && !holdsBytes(rest);
    }
    if (message.type === 'rev' && message.message !== undefined) {
        const { message: inner, ...rest } = message;
        return bytesInPlace(inner as CborValue) && !holdsBytes(rest);
    }
    return !holdsBytes(message);
};

/**
 * Reads the opened bytes of a message of an open link, or undefined when they are not one: not CBOR, past the
 * limits, no `type`, or a byte string anywhere but a `say`'s data, so every other value is JSON's
 */
export function decodeLinkMessage(bytes: Uint8Array): LinkMessage | undefined {
    let value: CborValue;
    try {
        value = decodeCbor(bytes, { bytes: true, limits: CBOR_LIMITS });
    } catch {
        return undefined;
    }
    if (value === null || typeof value !== 'object' || Array.isArray(value) || value instanceof Uint8Array || typeof value.type !== 'string') return undefined;
    if (value.type === 'frame') {
        if (!Array.isArray(value.messages) || !value.messages.every(bytesInPlace)) return undefined;
    } else if (holdsBytes(value)) return undefined;
    return value as unknown as LinkMessage;
}
