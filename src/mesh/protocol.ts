import type { Change } from '@punyts/state-sync';
import type { Access, GrantKey, RelayBatch, RelayWelcome, WriteOutcome } from '@punyts/state-sync/authority';
import type { NodeRole } from './certificates.js';

/** The version of the mesh protocol, named in `link-hello` */
export const MESH_PROTOCOL = 1;

/**
 * What a parent tells a node below it about the statenet (docs/design/statenet-relays.md, "The directory"), so a relay
 * learns what it needs without database credentials: the nodes, who owns which namespace, and the public keys that
 * sign grants
 */
export interface DirectoryView {
    readonly nodes: ReadonlyArray<{ readonly id: string; readonly role: NodeRole; readonly region: string; readonly endpoint: string }>;
    readonly owners: Readonly<Record<string, { readonly node: string; readonly epoch: number }>>;
    readonly keys: Readonly<Record<string, GrantKey>>;
}

/** The handshake: each end shows its certificate and signs the other's challenge */
export type LinkHello = { type: 'link-hello'; nodeId: string; role: NodeRole; protocol: number; region: string; certificate: string; nonce: string };
export type LinkWelcome = { type: 'link-welcome'; nodeId: string; certificate: string; nonce: string; proof: string; directory: DirectoryView };
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
    | { type: 'directory'; directory: DirectoryView };

/** Many messages from one moment, of many namespaces, in one WebSocket message */
export type MeshFrame = { type: 'frame'; messages: MeshMessage[] };

export type LinkMessage = LinkHello | LinkWelcome | LinkProof | LinkRefused | MeshFrame | { type: 'ping' } | { type: 'pong' };

/** Parses one WebSocket message of a link, or undefined when it is not one */
export function parseLinkMessage(data: unknown): LinkMessage | undefined {
    try {
        const text = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : Array.isArray(data) ? Buffer.concat(data as Buffer[]).toString('utf8') : Buffer.from(data as ArrayBuffer).toString('utf8');
        const message = JSON.parse(text) as { type?: unknown };
        return typeof message === 'object' && message !== null && typeof message.type === 'string' ? (message as LinkMessage) : undefined;
    } catch {
        return undefined;
    }
}
