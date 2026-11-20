import { FencedError } from '@punyts/state-sync/authority';
import type { EpochStorage, StoredNamespace, Upstream } from '@punyts/state-sync/authority';
import type { KeyPair, NodeRole } from './certificates.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer, MeshServerOptions } from './MeshServer.js';
import type { DirectoryView, LoadOutcome, PersistOutcome } from './protocol.js';

/**
 * What saves and loads namespaces for a node: an aggregate in the same process, or a node's link up the mesh to one
 * (`MeshUpstream` has both methods). `node` names the node that orders the namespace, when a relay passes it on
 */
export interface AggregateLink {
    persist(address: string, stored: StoredNamespace, epoch: number, node?: string): Promise<PersistOutcome>;
    load(address: string, node?: string): Promise<LoadOutcome>;
}

export interface AggregateOptions {
    /**
     * Whether a node may save a namespace under an epoch: an aggregate with the directory answers from who owns the
     * address. Without it the storage's fence is the only check
     */
    readonly mayPersist?: (address: string, node: string | undefined, epoch: number) => Promise<boolean> | boolean;
    readonly report?: (message: string) => void;
}

/** What an aggregate says about its work, for its owner and tests */
export interface AggregateStats {
    /** Saves that reached the storage */
    readonly saves: number;
    /** Saves answered without one: resent, or folded into a later save of the same namespace */
    readonly folded: number;
    readonly fenced: number;
}

export interface Aggregate extends AggregateLink {
    stats(): AggregateStats;
}

interface Waiter {
    readonly revision: number;
    readonly epoch: number;
    readonly done: (outcome: PersistOutcome) => void;
}

interface Lane {
    /** The newest save that landed, as far as this aggregate saw */
    head?: { revision: number; epoch: number };
    /** The save that goes next, with everyone waiting on it: a later save of the namespace replaces it */
    next?: { stored: StoredNamespace; epoch: number; waiters: Waiter[] };
    running: boolean;
}

/**
 * The only writer of state (docs/design/statenet-network.md, "Aggregates write to the database"): nodes send it their
 * namespaces to save, and its answer is what makes a change durable. The saves of a namespace run one at a time; those
 * that arrive while one runs are folded into one batch, the newest, and every node waiting on an earlier revision is
 * answered when it lands. A save at or below the revision that already landed under its epoch writes nothing (a resend
 * after a dropped link); a save under an older epoch is answered `fenced` and writes nothing, by this aggregate's own
 * count or by the storage's, which is the one that holds across aggregates
 */
export function createAggregate(storage: EpochStorage, options: AggregateOptions = {}): Aggregate {
    const lanes = new Map<string, Lane>();
    let saves = 0;
    let folded = 0;
    let fenced = 0;

    const fence = (epoch: number, current: number): PersistOutcome => {
        fenced++;
        return { result: 'fenced', epoch, current };
    };

    const run = async (address: string, lane: Lane): Promise<void> => {
        if (lane.running) return;
        lane.running = true;
        try {
            for (let batch = lane.next; batch !== undefined; batch = lane.next) {
                delete lane.next;
                let outcome: PersistOutcome;
                const landed = lane.head;
                if (landed !== undefined && batch.epoch === landed.epoch && batch.stored.revision <= landed.revision) {
                    // sent again while the first was being saved (its link dropped): it has landed by now
                    folded += batch.waiters.length;
                    for (const waiter of batch.waiters) waiter.done({ result: 'saved', revision: landed.revision });
                    continue;
                }
                try {
                    await storage.save(address, batch.stored, batch.epoch);
                    saves++;
                    lane.head = { revision: batch.stored.revision, epoch: batch.epoch };
                    outcome = { result: 'saved', revision: batch.stored.revision };
                } catch (error) {
                    if (error instanceof FencedError) {
                        lane.head = { revision: lane.head?.revision ?? 0, epoch: Math.max(error.current, lane.head?.epoch ?? 0) };
                        outcome = fence(batch.epoch, error.current);
                    } else {
                        const reason = error instanceof Error ? error.message : String(error);
                        options.report?.(`Saving ${address} failed: ${reason}`);
                        outcome = { result: 'failed', reason };
                    }
                }
                folded += Math.max(0, batch.waiters.length - 1);
                for (const waiter of batch.waiters) waiter.done(outcome);
            }
        } finally {
            lane.running = false;
        }
    };

    return {
        persist: async (address, stored, epoch, node) => {
            if (options.mayPersist !== undefined && !(await options.mayPersist(address, node, epoch))) return { result: 'failed', reason: `${node ?? 'this node'} does not order ${address}` };
            let lane = lanes.get(address);
            if (lane === undefined) lanes.set(address, (lane = { running: false }));
            const head = lane.head;
            if (head !== undefined && epoch < head.epoch) return fence(epoch, head.epoch);
            if (head !== undefined && epoch === head.epoch && stored.revision <= head.revision) {
                folded++;
                return { result: 'saved', revision: head.revision };
            }
            const next = lane.next;
            if (next !== undefined && epoch < next.epoch) return fence(epoch, next.epoch);
            const held = lane;
            return new Promise<PersistOutcome>((done) => {
                const waiter: Waiter = { revision: stored.revision, epoch, done };
                if (next === undefined) held.next = { stored, epoch, waiters: [waiter] };
                else if (epoch > next.epoch) {
                    // a new owner's save takes the place of the old owner's, which is answered as the storage would
                    for (const old of next.waiters) old.done(fence(old.epoch, epoch));
                    held.next = { stored, epoch, waiters: [waiter] };
                } else {
                    if (stored.revision >= next.stored.revision) next.stored = stored;
                    next.waiters.push(waiter);
                }
                void run(address, held);
            });
        },
        load: async (address) => {
            try {
                const stored = await storage.load(address);
                return stored === undefined ? { ok: true } : { ok: true, stored };
            } catch (error) {
                return { ok: false, reason: error instanceof Error ? error.message : String(error) };
            }
        },
        stats: () => ({ saves, folded, fenced })
    };
}

/**
 * A node's storage that keeps nothing itself (docs/design/statenet-network.md): a save goes to the aggregate and is
 * done when the aggregate says it landed, which is when the authority calls the change durable; a save the aggregate
 * fences throws `FencedError`, so the node stops ordering the namespace. `node` is the node's own id, for an
 * aggregate that checks who orders what
 */
export function createAggregateStorage(link: AggregateLink, node?: string): EpochStorage {
    return {
        load: async (address) => {
            const outcome = await link.load(address, node);
            if (!outcome.ok) throw new Error(`Loading ${address} from the aggregate failed: ${outcome.reason}`);
            return outcome.stored;
        },
        save: async (address, stored, epoch) => {
            const outcome = await link.persist(address, stored, epoch, node);
            if (outcome.result === 'fenced') throw new FencedError(address, outcome.epoch, outcome.current);
            if (outcome.result === 'failed') throw new Error(`Saving ${address} at the aggregate failed: ${outcome.reason}`);
        }
    };
}

export interface AggregateNodeOptions extends AggregateOptions {
    readonly nodeId: string;
    readonly keys: KeyPair;
    readonly certificate: string | (() => string);
    readonly authorityPublicKey: string;
    /** Where the namespaces are kept: Postgres, files, memory */
    readonly storage: EpochStorage;
    /** The directory the relays below learn; empty by default */
    readonly directory?: () => DirectoryView;
    readonly host?: string;
    /** The port of the relays' links; 0 for any */
    readonly port?: number;
    readonly tls?: MeshServerOptions['tls'];
    readonly requireListed?: readonly NodeRole[];
    readonly reread?: MeshServerOptions['reread'];
    readonly certify?: MeshServerOptions['certify'];
}

export interface AggregateNode {
    readonly nodeId: string;
    readonly url: string;
    /** The aggregate itself, for a node in the same process */
    readonly aggregate: Aggregate;
    readonly mesh: MeshServer;
    close(): Promise<void>;
}

const EMPTY: DirectoryView = { nodes: [], owners: {}, keys: {} };

/** An aggregate serves no subscriptions yet: a relay that asks is told so, and keeps to its other parents */
const noSubscriptions: Upstream = {
    subscribe: (_address, _prefix, _revision, listener) => {
        queueMicrotask(() => listener.closed?.('an aggregate serves no subscriptions'));
        return { renew: () => undefined, close: () => undefined };
    }
};

/**
 * An aggregate process (docs/design/statenet-network.md, "Roles and who links to whom"): relays link to it, and it
 * saves and loads what they pass up from their nodes. Only relays can open a link to it (`mayLink`)
 */
export async function startAggregateNode(options: AggregateNodeOptions): Promise<AggregateNode> {
    const aggregate = createAggregate(options.storage, { ...(options.mayPersist === undefined ? {} : { mayPersist: options.mayPersist }), ...(options.report === undefined ? {} : { report: options.report }) });
    const mesh = await startMeshServer({
        nodeId: options.nodeId,
        role: 'aggregate',
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        upstream: noSubscriptions,
        write: async () => ({ result: 'rejected', reason: 'an aggregate orders nothing: write to the node that owns the namespace' }),
        directory: options.directory ?? (() => EMPTY),
        persist: (address, stored, epoch, node) => aggregate.persist(address, stored, epoch, node),
        load: (address, node) => aggregate.load(address, node),
        ...(options.host === undefined ? {} : { host: options.host }),
        port: options.port ?? 0,
        ...(options.tls === undefined ? {} : { tls: options.tls }),
        ...(options.requireListed === undefined ? {} : { requireListed: options.requireListed }),
        ...(options.reread === undefined ? {} : { reread: options.reread }),
        ...(options.certify === undefined ? {} : { certify: options.certify })
    });
    return {
        nodeId: options.nodeId,
        url: mesh.url,
        aggregate,
        mesh,
        close: () => mesh.close()
    };
}
