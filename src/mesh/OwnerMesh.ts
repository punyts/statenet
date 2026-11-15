import type { Change } from '@punyts/state-sync';
import { createAuthorityUpstream, createRelay, OwnerNode } from '@punyts/state-sync/authority';
import type { Access, Authority, AuthorityConnection, AuthoritySession, ClaimPolicy, GrantKey, NamespaceHost, NodePeer, OwnershipStore, Placement, Relay, RelayListener, RelaySubscription, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import type { KeyPair } from './certificates.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer } from './MeshServer.js';
import { createMeshUpstream } from './MeshUpstream.js';
import type { MeshUpstream } from './MeshUpstream.js';
import type { DirectoryView } from './protocol.js';

export interface PeerLinksOptions {
    readonly nodeId: string;
    readonly region?: string;
    readonly keys: KeyPair;
    readonly certificate: string;
    readonly authorityPublicKey: string;
    /** The directory as it is now: a peer is linked at the endpoint it names, and only while it names one */
    readonly directory: () => DirectoryView;
    /** How long a write sent to a peer may go unanswered before it is rejected; 15 seconds by default */
    readonly writeTimeoutMs?: number;
    readonly report?: (message: string) => void;
}

export interface PeerLinks {
    /** The link to another owner node, made on first use; undefined when the directory does not name it (it is gone) */
    link(nodeId: string): MeshUpstream | undefined;
    /** The node as an `OwnerNode` reaches it: writes, claims and handoffs over its link */
    peer(nodeId: string): NodePeer | undefined;
    /** The nodes linked now */
    linked(): string[];
    close(): void;
}

/**
 * The links of an owner node to the other owner nodes of the statenet (docs/design/statenet-relays.md, "Topology, in
 * steps", 3): one mesh link per peer, opened when this node first needs it (a write to forward, a view to tunnel, a
 * claim) at the endpoint the directory names, and dropped when the directory stops naming it. A link to a peer never
 * falls over to another node: a peer that is gone is gone, and its namespaces are taken over once their leases run out
 */
export function createPeerLinks(options: PeerLinksOptions): PeerLinks {
    const links = new Map<string, { endpoint: string; upstream: MeshUpstream }>();
    const writeTimeoutMs = options.writeTimeoutMs ?? 15_000;

    /** A peer's answer, or `fallback` when it does not come in time */
    const within = <T>(answer: Promise<T>, fallback: T): Promise<T> =>
        new Promise<T>((resolve) => {
            const timer = setTimeout(() => resolve(fallback), writeTimeoutMs);
            (timer as { unref?: () => void }).unref?.();
            void answer.then((value) => {
                clearTimeout(timer);
                resolve(value);
            });
        });

    const link = (nodeId: string): MeshUpstream | undefined => {
        if (nodeId === options.nodeId) return undefined;
        const endpoint = options.directory().nodes.find((node) => node.id === nodeId && node.role === 'owner')?.endpoint;
        const found = links.get(nodeId);
        if (found !== undefined && found.endpoint === endpoint) return found.upstream;
        found?.upstream.close();
        links.delete(nodeId);
        if (endpoint === undefined || endpoint === '') return undefined;
        const upstream = createMeshUpstream({
            nodeId: options.nodeId,
            role: 'owner',
            ...(options.region === undefined ? {} : { region: options.region }),
            keys: options.keys,
            certificate: options.certificate,
            authorityPublicKey: options.authorityPublicKey,
            parents: [endpoint],
            followDirectory: false,
            ...(options.report === undefined ? {} : { report: options.report })
        });
        links.set(nodeId, { endpoint, upstream });
        return upstream;
    };

    return {
        link,
        peer: (nodeId) => {
            const upstream = link(nodeId);
            if (upstream === undefined) return undefined;
            return {
                write: (address: string, change: Change) =>
                    new Promise<WriteOutcome>((resolve) => {
                        const timer = setTimeout(() => resolve({ result: 'rejected', reason: `node ${nodeId} did not answer` }), writeTimeoutMs);
                        (timer as { unref?: () => void }).unref?.();
                        void upstream.write(address, change).then((outcome) => {
                            clearTimeout(timer);
                            resolve(outcome);
                        });
                    }),
                claim: (address: string) => within(upstream.take(address), false),
                handoff: (address: string) => within(upstream.handoff(address), undefined)
            };
        },
        linked: () => [...links.keys()],
        close: () => {
            for (const { upstream } of links.values()) upstream.close();
            links.clear();
        }
    };
}

export interface PlacedUpstreamOptions {
    /** Where a namespace is ordered (`OwnerNode.place`) */
    readonly place: (address: string) => Promise<Placement>;
    /** What serves the namespaces this node owns */
    readonly local: Upstream;
    /** What serves those another node owns: its link */
    readonly remote: (nodeId: string) => Upstream | undefined;
    /** The wait before subscribing again after the namespace moved; 50 ms times the attempt by default */
    readonly retryMs?: number;
    /** How many times in a row it subscribes again before it gives up and closes the listener; 5 by default */
    readonly attempts?: number;
}

/**
 * An upstream that follows a namespace's owner: it subscribes where the namespace is ordered, here or at the owner
 * node, and when that subscription ends because the namespace moved (a handoff, a lapsed lease, a restarted authority)
 * it finds the owner again and subscribes there from the revision it had. A relay over it sees a new welcome, which
 * under a new epoch starts its cache over from a snapshot
 */
export function createPlacedUpstream(options: PlacedUpstreamOptions): Upstream {
    const retryMs = options.retryMs ?? 50;
    const attempts = options.attempts ?? 5;
    return {
        subscribe: (address, prefix, revision, listener, access): RelaySubscription => {
            let current: RelaySubscription | undefined;
            let closed = false;
            let have = revision;
            let tries = 0;
            const end = (reason: string): void => {
                if (closed) return;
                closed = true;
                listener.closed?.(reason);
            };
            const inner: RelayListener = {
                welcome: (welcome) => {
                    have = welcome.revision;
                    tries = 0;
                    listener.welcome(welcome);
                },
                changes: (batch) => {
                    have = batch.revision;
                    listener.changes(batch);
                },
                durable: (to) => listener.durable?.(to),
                revoked: (grantId, expires) => listener.revoked?.(grantId, expires),
                closed: (reason) => {
                    current = undefined;
                    if (closed) return;
                    if (reason === 'revoked' || tries >= attempts) return end(reason);
                    tries++;
                    const timer = setTimeout(start, retryMs * tries);
                    (timer as { unref?: () => void }).unref?.();
                }
            };
            function start(): void {
                if (closed) return;
                options.place(address).then(
                    (placement) => {
                        if (closed) return;
                        const upstream = placement.here ? options.local : options.remote(placement.node);
                        if (upstream === undefined) return inner.closed?.(`node ${placement.here ? 'here' : placement.node} cannot be reached`);
                        current = upstream.subscribe(address, prefix, have, inner, access);
                    },
                    (error: unknown) => inner.closed?.(error instanceof Error ? error.message : String(error))
                );
            }
            start();
            return {
                renew: () => current?.renew(),
                close: () => {
                    closed = true;
                    current?.close();
                    current = undefined;
                }
            };
        }
    };
}

export interface OwnerMeshOptions {
    readonly nodeId: string;
    readonly region?: string;
    readonly keys: KeyPair;
    readonly certificate: string;
    readonly authorityPublicKey: string;
    /** Where ownership is kept (`PgOwnershipStore`) */
    readonly ownership: OwnershipStore;
    /** The node's authorities: an app server's own (with its logic and storage) */
    readonly host: NamespaceHost;
    /** The directory as it is now: the nodes with their endpoints, the owners and the keys that sign grants */
    readonly directory: () => DirectoryView;
    readonly policy?: ClaimPolicy;
    readonly leaseMs?: number;
    readonly onOwner?: (address: string, owning: boolean, epoch: number) => void;
    /** The address the links of peers and relays come in on */
    readonly listenHost?: string;
    /** 0 for any free port */
    readonly port?: number;
    readonly tls?: { readonly key: string; readonly cert: string };
    /** Checks the grants of reads by relays' listeners at this node's relay, with these keys; every read is served without it */
    readonly grants?: { keyOf: (kid: string) => GrantKey | undefined };
    readonly writeTimeoutMs?: number;
    readonly report?: (message: string) => void;
}

export interface OwnerMesh {
    readonly nodeId: string;
    readonly node: OwnerNode;
    readonly peers: PeerLinks;
    /** This node's relay, over the upstream that follows each namespace's owner */
    readonly relay: Relay;
    readonly mesh: MeshServer;
    /** The URL peers and relays link to, as the server listens (advertise the public one in the directory) */
    readonly url: string;
    /**
     * A session of a namespace for a client of this node: with the authority here when this node owns it (or takes
     * it), tunnelled to the owner otherwise. `here` is called with the authority to connect when it is here
     */
    connect(address: string, connection: AuthorityConnection, access: Access | undefined, here: () => Promise<Pick<Authority, 'connect'>>): Promise<AuthoritySession>;
    /** Tells the links that the directory changed */
    directoryChanged(): void;
    close(): Promise<void>;
}

/**
 * An owner node of the statenet with its mesh (docs/design/statenet-relays.md, "Topology, in steps", 2 and 3): an
 * `OwnerNode` over the host's authorities, links to the other owner nodes for forwarded writes, tunnelled sessions and
 * claims, a relay that follows each namespace's owner, and a mesh server that peers and relays link to
 */
export async function startOwnerMesh(options: OwnerMeshOptions): Promise<OwnerMesh> {
    const peers = createPeerLinks({
        nodeId: options.nodeId,
        ...(options.region === undefined ? {} : { region: options.region }),
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        directory: options.directory,
        ...(options.writeTimeoutMs === undefined ? {} : { writeTimeoutMs: options.writeTimeoutMs }),
        ...(options.report === undefined ? {} : { report: options.report })
    });
    const node = new OwnerNode({
        id: options.nodeId,
        ownership: options.ownership,
        host: options.host,
        link: (id) => peers.peer(id),
        ...(options.policy === undefined ? {} : { policy: options.policy }),
        ...(options.leaseMs === undefined ? {} : { leaseMs: options.leaseMs }),
        ...(options.onOwner === undefined ? {} : { onOwner: options.onOwner })
    });
    const local = createAuthorityUpstream(
        async (address) => (node.owns(address) ? node.authorityOf(address) : undefined),
        (address) => node.epochOf(address)
    );
    const placed = createPlacedUpstream({ place: (address) => node.place(address), local, remote: (id) => peers.link(id) });
    const isNode = (subject: string): boolean => options.directory().nodes.some((each) => each.id === subject);
    const relay = createRelay(placed, { node: options.nodeId, trustedNodes: isNode, ...(options.grants === undefined ? {} : { grants: options.grants }) });
    const mesh = await startMeshServer({
        nodeId: options.nodeId,
        role: 'owner',
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        upstream: relay,
        write: (address, change, _access, via) => node.write(address, change, via),
        directory: options.directory,
        sessions: async (address) => {
            const authority = node.owns(address) ? await node.authorityOf(address) : undefined;
            return authority ?? `${address} is not owned by ${options.nodeId}`;
        },
        handoff: (address, to) => node.handoff(address, to),
        take: (address) => node.claim(address),
        ...(options.listenHost === undefined ? {} : { host: options.listenHost }),
        port: options.port ?? 0,
        ...(options.tls === undefined ? {} : { tls: options.tls })
    });
    return {
        nodeId: options.nodeId,
        node,
        peers,
        relay,
        mesh,
        url: mesh.url,
        connect: async (address, connection, access, here) => {
            const placement = await node.place(address);
            if (placement.here) return (await here()).connect(connection, access);
            const link = peers.link(placement.node);
            if (link === undefined) throw new Error(`node ${placement.node} cannot be reached`);
            return link.open(address, connection, access);
        },
        directoryChanged: () => mesh.directoryChanged(),
        close: async () => {
            relay.close();
            await node.stop();
            peers.close();
            await mesh.close();
        }
    };
}
