import { report } from '@punyts/core';
import { FencedError, isResave, serialOf } from '@punyts/state-sync/authority';
import type { EpochStorage, StoredNamespace, Upstream } from '@punyts/state-sync/authority';
import { verifyNodeCertificate } from './certificates.js';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer, MeshServerOptions, OpenedLink } from './MeshServer.js';
import type { DirectoryView, LoadOutcome, PersistOutcome, RemoveOutcome } from './protocol.js';
import { createRelayTree } from './relayTree.js';

/** The category of an aggregate's reports: addresses, epochs, revisions, counts and durations, never values */
const CATEGORY = 'statenet.aggregate';

/** An aggregate's storage: namespaces under epochs, and, where it can, removing one whole */
export type AggregateStorageBackend = EpochStorage & { remove?(address: string, epoch?: number): Promise<void> };

/**
 * What saves and loads namespaces for a node: an aggregate in the same process, or a node's link up the mesh to one
 * (`MeshUpstream` has both methods). `node` names the node that orders the namespace, when a relay passes it on
 */
export interface AggregateLink {
    persist(address: string, stored: StoredNamespace, epoch: number, node?: string): Promise<PersistOutcome>;
    load(address: string, node?: string): Promise<LoadOutcome>;
    /** Removes a namespace whole, for the node that holds it under `epoch` */
    remove(address: string, epoch: number, node?: string): Promise<RemoveOutcome>;
}

export interface AggregateOptions {
    /**
     * Whether a node may save a namespace under an epoch: an aggregate with the directory answers from who owns the
     * address. Without it the storage's fence is the only check
     */
    readonly mayPersist?: (address: string, node: string | undefined, epoch: number) => Promise<boolean> | boolean;
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
    head?: { revision: number; serial: number; epoch: number };
    /** The save that goes next, with everyone waiting on it: a later save of the namespace replaces it */
    next?: { stored: StoredNamespace; epoch: number; waiters: Waiter[] };
    running: boolean;
    /** The saves under way, for a removal to wait out */
    work?: Promise<void>;
}

/**
 * The only writer of state (docs/design/statenet-network.md, "Aggregates write to the database"): nodes send it their
 * namespaces to save, and its answer is what makes a change durable. The saves of a namespace run one at a time; those
 * that arrive while one runs are folded into one batch, the newest, and every node waiting on an earlier revision is
 * answered when it lands. A save that already landed under its epoch (`isResave`: at or below its revision and serial,
 * which counts what takes no revision, such as $.server writes and requests) writes nothing (a resend
 * after a dropped link); a save under an older epoch is answered `fenced` and writes nothing, by this aggregate's own
 * count or by the storage's, which is the one that holds across aggregates
 */
export function createAggregate(storage: AggregateStorageBackend, options: AggregateOptions = {}): Aggregate {
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
                if (landed !== undefined && batch.epoch === landed.epoch && isResave(batch.stored, landed)) {
                    // sent again while the first was being saved (its link dropped): it has landed by now
                    folded += batch.waiters.length;
                    report(CATEGORY, 'Folded %s saves of %s: revision %s has landed', [batch.waiters.length, address, landed.revision]);
                    for (const waiter of batch.waiters) waiter.done({ result: 'saved', revision: landed.revision });
                    continue;
                }
                const start = performance.now();
                try {
                    await storage.save(address, batch.stored, batch.epoch);
                    saves++;
                    report(CATEGORY, 'Saved %s at revision %s under epoch %s in %s ms, for %s waiting', [address, batch.stored.revision, batch.epoch, Math.round((performance.now() - start) * 10) / 10, batch.waiters.length]);
                    lane.head = { revision: batch.stored.revision, serial: serialOf(batch.stored), epoch: batch.epoch };
                    outcome = { result: 'saved', revision: batch.stored.revision };
                } catch (error) {
                    if (error instanceof FencedError) {
                        lane.head = { revision: lane.head?.revision ?? 0, serial: lane.head?.serial ?? 0, epoch: Math.max(error.current, lane.head?.epoch ?? 0) };
                        report(CATEGORY, 'Fenced a save of %s under epoch %s: the storage has epoch %s', [address, batch.epoch, error.current]);
                        outcome = fence(batch.epoch, error.current);
                    } else {
                        const reason = error instanceof Error ? error.message : String(error);
                        report.error('Saving %s at revision %s failed: %s', [address, batch.stored.revision, error instanceof Error ? error.name : typeof error]);
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
            if (options.mayPersist !== undefined && !(await options.mayPersist(address, node, epoch))) {
                report.warning('Refused a save of %s from %s: it does not order it', [address, node ?? 'a node']);
                return { result: 'failed', reason: `${node ?? 'this node'} does not order ${address}` };
            }
            let lane = lanes.get(address);
            if (lane === undefined) lanes.set(address, (lane = { running: false }));
            const head = lane.head;
            if (head !== undefined && epoch < head.epoch) {
                report(CATEGORY, 'Fenced a save of %s under epoch %s: it has epoch %s', [address, epoch, head.epoch]);
                return fence(epoch, head.epoch);
            }
            if (head !== undefined && epoch === head.epoch && isResave(stored, head)) {
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
                    if (isResave(next.stored, stored)) next.stored = stored;
                    next.waiters.push(waiter);
                }
                held.work = run(address, held);
            });
        },
        remove: async (address, epoch, node) => {
            if (options.mayPersist !== undefined && !(await options.mayPersist(address, node, epoch))) return { result: 'failed', reason: `${node ?? 'this node'} does not order ${address}` };
            if (storage.remove === undefined) return { result: 'failed', reason: 'this aggregate\'s storage removes nothing' };
            let lane = lanes.get(address);
            if (lane === undefined) lanes.set(address, (lane = { running: false }));
            // after the saves that came before it, so none of them lands on what was removed
            while (lane.running || lane.next !== undefined) await lane.work;
            if (lane.head !== undefined && epoch < lane.head.epoch) return fence(epoch, lane.head.epoch) as RemoveOutcome;
            try {
                await storage.remove(address, epoch);
            } catch (error) {
                if (error instanceof FencedError) {
                    lane.head = { revision: lane.head?.revision ?? 0, serial: lane.head?.serial ?? 0, epoch: Math.max(error.current, lane.head?.epoch ?? 0) };
                    return fence(epoch, error.current) as RemoveOutcome;
                }
                return { result: 'failed', reason: error instanceof Error ? error.message : String(error) };
            }
            // what landed is gone, and a save from before the removal's epoch stays refused
            lane.head = { revision: 0, serial: 0, epoch: Math.max(epoch, lane.head?.epoch ?? 0) };
            return { result: 'removed' };
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
export function createAggregateStorage(link: AggregateLink, node?: string): EpochStorage & { remove(address: string, epoch?: number): Promise<void> } {
    return {
        remove: async (address, epoch = 0) => {
            const outcome = await link.remove(address, epoch, node);
            if (outcome.result === 'fenced') throw new FencedError(address, outcome.epoch, outcome.current);
            if (outcome.result === 'failed') throw new Error(`Removing ${address} at the aggregate failed: ${outcome.reason}`);
        },
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

/**
 * A relay of the aggregate's tree, as the directory lists it (docs/design/statenet-relay-directory.md,
 * docs/design/statenet-relay-chains.md): everything but `parent` comes from the relay's certificate, and `parent` is
 * the aggregate for a relay linked to it, or the relay it is linked to for one further down
 */
export interface ListedRelay {
    readonly id: string;
    readonly region: string;
    readonly group: string;
    readonly endpoint: string;
    readonly publicKey: string;
    readonly parent: string;
}

/** Where an aggregate lists the relays linked to it: the database's directory */
export interface RelayListing {
    /** The relay's first link opened, here or on a relay of the tree: list it */
    list(relay: ListedRelay): Promise<void>;
    /** The relay's last link closed, or the link of a relay it is under: take it out */
    unlist(nodeId: string): Promise<void>;
}

export interface AggregateNodeOptions extends AggregateOptions {
    readonly nodeId: string;
    readonly keys: KeyPair;
    readonly certificate: string | (() => string);
    readonly authorityPublicKey: string;
    /** Where the namespaces are kept: Postgres, files, memory */
    readonly storage: AggregateStorageBackend;
    /** The directory the relays below learn; empty by default */
    readonly directory?: () => DirectoryView;
    readonly host?: string;
    /** The port of the relays' links; 0 for any */
    readonly port?: number;
    readonly tls?: MeshServerOptions['tls'];
    readonly requireListed?: readonly NodeRole[];
    readonly reread?: MeshServerOptions['reread'];
    readonly certify?: MeshServerOptions['certify'];
    /** Lists the relays linked here and below them while they are; without it nothing is listed */
    readonly relays?: RelayListing;
}

export interface AggregateNode {
    readonly nodeId: string;
    readonly url: string;
    /** The aggregate itself, for a node in the same process */
    readonly aggregate: Aggregate;
    readonly mesh: MeshServer;
    /** The relays linked here and below them now, as they are listed, each after its parent */
    relays(): ListedRelay[];
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
    const aggregate = createAggregate(options.storage, { ...(options.mayPersist === undefined ? {} : { mayPersist: options.mayPersist }) });
    /** Each relay linked here, with its count of open links: a relay that dials again may open a link before its old one closes */
    const linked = new Map<string, number>();
    /** Every listed relay under its parent: this aggregate for a relay linked here, the relay it is linked to for one below */
    const tree = createRelayTree<ListedRelay>(options.nodeId);
    /** Each relay's listing and unlisting, in the order its links opened and closed */
    const pending = new Map<string, Promise<void>>();
    const inTurn = (id: string, step: () => Promise<void>, what: string): void => {
        const next = (pending.get(id) ?? Promise.resolve())
            .then(step)
            .catch((error: unknown) => report.warning('Aggregate %s could not %s relay %s: %s', [options.nodeId, what, id, error instanceof Error ? error.name : 'error']));
        pending.set(id, next);
        void next.then(() => {
            if (pending.get(id) === next) pending.delete(id);
        });
    };
    const listing = options.relays;
    const list = (claims: NodeCertificateClaims, parent: string): void => {
        if (listing === undefined) return;
        const relay: ListedRelay = { id: claims.nodeId, region: claims.region, group: claims.group ?? '', endpoint: claims.endpoint ?? '', publicKey: claims.publicKey, parent };
        if (!tree.link(relay.id, parent, relay)) return;
        report(CATEGORY, 'Aggregate %s lists relay %s of group %s under %s', [options.nodeId, relay.id, relay.group, parent]);
        inTurn(relay.id, () => listing.list(relay), 'list');
    };
    /** Takes a relay out with every relay under it */
    const unlist = (id: string): void => {
        if (listing === undefined) return;
        for (const gone of tree.unlink(id)) {
            report(CATEGORY, 'Aggregate %s takes relay %s out of the directory', [options.nodeId, gone]);
            inTurn(gone, () => listing.unlist(gone), 'unlist');
        }
    };
    const opened = (peer: NodeCertificateClaims, link: OpenedLink): void => {
        if (peer.role !== 'relay' || link.routing) return;
        const links = (linked.get(peer.nodeId) ?? 0) + 1;
        linked.set(peer.nodeId, links);
        if (links === 1) list(peer, options.nodeId);
    };
    const closed = (peer: NodeCertificateClaims, link: OpenedLink): void => {
        const links = peer.role !== 'relay' || link.routing ? undefined : linked.get(peer.nodeId);
        if (links === undefined) return;
        if (links > 1) return void linked.set(peer.nodeId, links - 1);
        linked.delete(peer.nodeId);
        unlist(peer.nodeId);
    };
    /**
     * A relay below passed up a relay linked to it: listed under `via` when the aggregate checked its certificate itself
     * and `via` is listed under the relay on this link (a relay vouches only for its own branch)
     */
    const relayLinked = (certificate: string, via: string, peer: NodeCertificateClaims): void => {
        const checked = verifyNodeCertificate(certificate, options.authorityPublicKey, Date.now());
        if (!checked.ok || checked.claims.role !== 'relay') {
            report.warning('Aggregate %s lists no relay passed up by %s: %s', [options.nodeId, peer.nodeId, checked.ok ? 'it is not a relay' : checked.reason]);
            return;
        }
        if (linked.has(checked.claims.nodeId) || tree.branchOf(via) !== peer.nodeId || tree.parentOf(checked.claims.nodeId) === via) return;
        list(checked.claims, via);
    };
    const relayUnlinked = (id: string, via: string, peer: NodeCertificateClaims): void => {
        if (tree.parentOf(id) === via && tree.branchOf(via) === peer.nodeId) unlist(id);
    };
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
        remove: (address, epoch, node) => aggregate.remove(address, epoch, node),
        ...(options.host === undefined ? {} : { host: options.host }),
        port: options.port ?? 0,
        ...(options.tls === undefined ? {} : { tls: options.tls }),
        ...(options.requireListed === undefined ? {} : { requireListed: options.requireListed }),
        ...(options.reread === undefined ? {} : { reread: options.reread }),
        ...(options.certify === undefined ? {} : { certify: options.certify }),
        onLinkOpened: opened,
        onLinkClosed: closed,
        relayLinked,
        relayUnlinked
    });
    return {
        nodeId: options.nodeId,
        url: mesh.url,
        aggregate,
        mesh,
        relays: () => tree.walk().map((entry) => entry.value),
        close: async () => {
            await mesh.close();
            await Promise.all([...pending.values()]);
        }
    };
}
