import { report } from '@punyts/core';
import type { Change } from '@punyts/state-sync';
import { createAuthorityUpstream, createRelay, OwnerNode } from '@punyts/state-sync/authority';
import type { Access, Authority, AuthorityConnection, AuthoritySession, ClaimPolicy, GrantKey, NamespaceHost, NodePeer, OwnershipStore, Placement, Relay, RelayListener, RelaySubscription, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer } from './MeshServer.js';
import { createMeshUpstream, reachThrough } from './MeshUpstream.js';
import type { MeshUpstream } from './MeshUpstream.js';
import type { DirectoryView } from './protocol.js';
import type { RouteTarget } from './serving.js';

/** The category of an owner node's mesh: node ids, addresses and attempts, never values */
const CATEGORY = 'statenet.owner';

export interface PeerLinksOptions {
    readonly nodeId: string;
    readonly region?: string;
    readonly keys: KeyPair;
    /** This node's certificate, or what gives its newest one */
    readonly certificate: string | (() => string);
    readonly authorityPublicKey: string;
    /** This node's group: a peer of the same group is linked directly, one of another is reached with `through` */
    readonly group?: string;
    /** The way to a node of another group: through this node's relay */
    readonly through?: (nodeId: string) => RouteTarget | undefined;
    /** The directory as it is now: a peer is linked at the endpoint it names, and only while it names one */
    readonly directory: () => DirectoryView;
    /** How long a write sent to a peer may go unanswered before it is rejected; 15 seconds by default */
    readonly writeTimeoutMs?: number;
}

export interface PeerLinks {
    /**
     * The way to another owner node: a link to it, made on first use, when it is of this node's group, and this node's
     * relay otherwise; undefined when the directory does not name it (it is gone), or there is no way
     */
    link(nodeId: string): RouteTarget | undefined;
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

    const link = (nodeId: string): RouteTarget | undefined => {
        if (nodeId === options.nodeId) return undefined;
        const listed = options.directory().nodes.find((node) => node.id === nodeId && node.role === 'owner');
        // nodes link to each other only inside a group: another group's node is reached through the relays
        if (listed !== undefined && (listed.group ?? '') !== (options.group ?? '')) {
            report(CATEGORY, 'Node %s reaches %s of group %s through its relay', [options.nodeId, nodeId, listed.group ?? '(none)']);
            return options.through?.(nodeId);
        }
        const endpoint = listed?.endpoint;
        const found = links.get(nodeId);
        if (found !== undefined && found.endpoint === endpoint) return found.upstream;
        found?.upstream.close();
        links.delete(nodeId);
        if (endpoint === undefined || endpoint === '') {
            report(CATEGORY, 'Node %s has no way to %s: the directory names no endpoint', [options.nodeId, nodeId]);
            return undefined;
        }
        report(CATEGORY, 'Node %s opens a link to its peer %s', [options.nodeId, nodeId]);
        const upstream = createMeshUpstream({
            nodeId: options.nodeId,
            role: 'owner',
            ...(options.group === undefined ? {} : { group: options.group }),
            ...(options.region === undefined ? {} : { region: options.region }),
            keys: options.keys,
            certificate: options.certificate,
            authorityPublicKey: options.authorityPublicKey,
            parents: [endpoint],
            followDirectory: false
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
                write: (address: string, change: Change, _via?: string, access?: Access) =>
                    new Promise<WriteOutcome>((resolve) => {
                        const timer = setTimeout(() => {
                            report.warning('Node %s did not answer a write of %s in %s ms', [nodeId, address, writeTimeoutMs]);
                            resolve({ result: 'rejected', reason: `node ${nodeId} did not answer` });
                        }, writeTimeoutMs);
                        (timer as { unref?: () => void }).unref?.();
                        void upstream.write(address, change, access).then((outcome) => {
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
                revoked: (entry, expires) => listener.revoked?.(entry, expires),
                closed: (reason) => {
                    current = undefined;
                    if (closed) return;
                    if (reason === 'revoked' || tries >= attempts) {
                        report(CATEGORY, 'Gave up following the owner of %s after %s attempts: %s', [address, tries, reason]);
                        return end(reason);
                    }
                    tries++;
                    report(CATEGORY, 'The subscription to %s ended (%s): finding its owner again, attempt %s', [address, reason, tries]);
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
                        report(CATEGORY, 'Subscribing to %s %s at %s from revision %s', [address, prefix, placement.here ? 'this node' : placement.node, have ?? 0]);
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
    /** This node's certificate, or what gives its newest one */
    readonly certificate: string | (() => string);
    /** The roles whose nodes must be in the directory to link here (`MeshServerOptions.requireListed`) */
    readonly requireListed?: readonly NodeRole[];
    /** Reads the directory again for a node that is not listed yet (`MeshServerOptions.reread`) */
    readonly reread?: () => Promise<void>;
    /** Answers a node's ask for its next certificate (`MeshServerOptions.certify`) */
    readonly certify?: (certificate: string, claims: NodeCertificateClaims, via: NodeCertificateClaims) => Promise<string | undefined>;
    readonly authorityPublicKey: string;
    /** This node's group (its certificate names it too): the nodes it links to directly */
    readonly group?: string;
    /**
     * The relays this node links up to, in order of preference (docs/design/statenet-network.md, S9): its way to the
     * nodes of other groups and, for a node that saves through one, to the aggregate. None by default
     */
    readonly relays?: readonly string[];
    /** Called each time the link to a relay opens, with the relay's node id: the node lists it as its `parent` */
    readonly onUplinked?: (relayId: string) => void;
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
}

export interface OwnerMesh {
    readonly nodeId: string;
    readonly node: OwnerNode;
    readonly peers: PeerLinks;
    /** This node's relay, over the upstream that follows each namespace's owner */
    readonly relay: Relay;
    readonly mesh: MeshServer;
    /** This node's link to its relay, when it has relays */
    readonly uplink: MeshUpstream | undefined;
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
    let uplink: MeshUpstream | undefined;
    const peers = createPeerLinks({
        nodeId: options.nodeId,
        ...(options.group === undefined ? {} : { group: options.group }),
        through: (nodeId) => (uplink === undefined ? undefined : reachThrough(uplink, nodeId)),
        ...(options.region === undefined ? {} : { region: options.region }),
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        directory: options.directory,
        ...(options.writeTimeoutMs === undefined ? {} : { writeTimeoutMs: options.writeTimeoutMs })
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
    const sessions = async (address: string): Promise<Pick<Authority, 'connect'> | string> => {
        const authority = node.owns(address) ? await node.authorityOf(address) : undefined;
        return authority ?? `${address} is not owned by ${options.nodeId}`;
    };
    // the relay is served what the peers are: it passes on what the nodes of other groups ask
    uplink =
        options.relays === undefined || options.relays.length === 0
            ? undefined
            : createMeshUpstream({
                  nodeId: options.nodeId,
                  role: 'owner',
                  ...(options.group === undefined ? {} : { group: options.group }),
                  ...(options.region === undefined ? {} : { region: options.region }),
                  keys: options.keys,
                  certificate: options.certificate,
                  authorityPublicKey: options.authorityPublicKey,
                  parents: options.relays,
                  followDirectory: false,
                  ...(options.onUplinked === undefined ? {} : { onLinked: options.onUplinked }),
                  serve: {
                      upstream: relay,
                      write: (address, change, access, via) => node.write(address, change, via, access),
                      sessions,
                      handoff: (address, to) => node.handoff(address, to),
                      take: (address) => node.claim(address)
                  }
              });
    const mesh = await startMeshServer({
        nodeId: options.nodeId,
        role: 'owner',
        ...(options.group === undefined ? {} : { group: options.group }),
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        upstream: relay,
        write: (address, change, access, via) => node.write(address, change, via, access),
        directory: options.directory,
        sessions: async (address) => {
            const authority = node.owns(address) ? await node.authorityOf(address) : undefined;
            return authority ?? `${address} is not owned by ${options.nodeId}`;
        },
        handoff: (address, to) => node.handoff(address, to),
        take: (address) => node.claim(address),
        ...(options.requireListed === undefined ? {} : { requireListed: options.requireListed }),
        ...(options.reread === undefined ? {} : { reread: options.reread }),
        ...(options.certify === undefined ? {} : { certify: options.certify }),
        ...(options.listenHost === undefined ? {} : { host: options.listenHost }),
        port: options.port ?? 0,
        ...(options.tls === undefined ? {} : { tls: options.tls })
    });
    report(CATEGORY, 'Owner node %s started at %s with %s relays', [options.nodeId, mesh.url, options.relays?.length ?? 0]);
    return {
        nodeId: options.nodeId,
        node,
        peers,
        relay,
        mesh,
        uplink,
        url: mesh.url,
        connect: async (address, connection, access, here) => {
            const placement = await node.place(address);
            report(CATEGORY, 'Node %s connects a session of %s %s', [options.nodeId, address, placement.here ? 'here' : `through ${placement.node}`]);
            if (placement.here) return (await here()).connect(connection, access);
            const link = peers.link(placement.node);
            if (link === undefined) throw new Error(`node ${placement.node} cannot be reached`);
            return link.open(address, connection, access);
        },
        directoryChanged: () => mesh.directoryChanged(),
        close: async () => {
            report(CATEGORY, 'Owner node %s stops, owning %s namespaces', [options.nodeId, node.owned().length]);
            relay.close();
            await node.stop();
            peers.close();
            uplink?.close();
            await mesh.close();
        }
    };
}
