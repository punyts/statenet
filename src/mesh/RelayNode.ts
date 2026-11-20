import { createRelay } from '@punyts/state-sync/authority';
import type { Relay } from '@punyts/state-sync/authority';
import type { KeyPair } from './certificates.js';
import { startClientServer } from './ClientServer.js';
import type { ClientServer, ClientServerOptions } from './ClientServer.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer } from './MeshServer.js';
import { createMeshUpstream, reachThrough } from './MeshUpstream.js';
import type { MeshUpstream, MeshUpstreamOptions } from './MeshUpstream.js';
import type { DirectoryView } from './protocol.js';
import type { RouteTarget } from './serving.js';

export interface RelayNodeOptions {
    readonly nodeId: string;
    readonly region?: string;
    readonly keys: KeyPair;
    readonly certificate: string;
    readonly authorityPublicKey: string;
    /** The relay's next certificate, when its parent gave one before the old one ended: keep it for the next start */
    readonly onCertificate?: (certificate: string) => void;
    /** The nodes to link up to, in order of preference */
    readonly parents: readonly string[];
    readonly host?: string;
    /** The port of the links of the relays below; 0 for any */
    readonly meshPort?: number;
    /** The port browsers connect to; 0 for any, undefined for none */
    readonly clientPort?: number;
    readonly accessOf?: ClientServerOptions['accessOf'];
    /** Checks the grants of reads at this relay, with the keys the directory gives (true by default) */
    readonly checkGrants?: boolean;
    readonly upstream?: Partial<MeshUpstreamOptions>;
    readonly leaseMs?: number;
}

export interface RelayNode {
    readonly nodeId: string;
    readonly meshUrl: string;
    readonly clientUrl: string | undefined;
    readonly relay: Relay;
    readonly upstream: MeshUpstream;
    readonly mesh: MeshServer;
    close(): Promise<void>;
}

const EMPTY: DirectoryView = { nodes: [], owners: {}, keys: {} };

/**
 * A relay process (docs/design/statenet-relays.md, "Topology, in steps", 2 and 4): its relay caches what the nodes and
 * browsers below it subscribe to, over its link up the mesh, and serves them: relays below over the mesh protocol,
 * browsers over the sync protocol. It reaches no database: the owners, the nodes and the keys that sign grants come
 * from its parent's directory, and it checks reads with those keys at its edge.
 *
 * Nodes link to it too (docs/design/statenet-network.md, "Groups"): an ask that names a node goes down that node's
 * link when it is linked here, and otherwise to the relay the directory says the node is under, over a link this relay
 * opens to it. What a node sends to be saved goes up, toward an aggregate
 */
export async function startRelayNode(options: RelayNodeOptions): Promise<RelayNode> {
    let directory: DirectoryView = EMPTY;
    let mesh: MeshServer | undefined;
    const upstream = createMeshUpstream({
        nodeId: options.nodeId,
        role: 'relay',
        ...(options.region === undefined ? {} : { region: options.region }),
        keys: options.keys,
        certificate: options.certificate,
        authorityPublicKey: options.authorityPublicKey,
        parents: options.parents,
        ...(options.onCertificate === undefined ? {} : { onCertificate: options.onCertificate }),
        ...options.upstream,
        onDirectory: (view) => {
            directory = view;
            mesh?.directoryChanged();
            options.upstream?.onDirectory?.(view);
        }
    });
    /** The links to the relays of other groups, opened when an ask first needs one */
    const relays = new Map<string, { endpoint: string; link: MeshUpstream }>();
    const route = (to: string, via: { readonly role: string }): RouteTarget | undefined => {
        const below = mesh?.peer(to);
        if (below !== undefined || via.role === 'relay') return below;
        const under = directory.nodes.find((node) => node.id === to)?.parent;
        if (under === undefined || under === options.nodeId) return undefined;
        const endpoint = directory.nodes.find((node) => node.id === under && node.role === 'relay')?.endpoint;
        const found = relays.get(under);
        if (found !== undefined && found.endpoint === endpoint) return reachThrough(found.link, to);
        found?.link.close();
        relays.delete(under);
        if (endpoint === undefined || endpoint === '') return undefined;
        const link = createMeshUpstream({
            nodeId: options.nodeId,
            role: 'relay',
            ...(options.region === undefined ? {} : { region: options.region }),
            keys: options.keys,
            certificate: () => upstream.certificate(),
            authorityPublicKey: options.authorityPublicKey,
            parents: [endpoint],
            followDirectory: false
        });
        relays.set(under, { endpoint, link });
        return reachThrough(link, to);
    };
    const relay = createRelay(upstream, {
        node: options.nodeId,
        trustedNodes: (subject) => directory.nodes.some((node) => node.id === subject),
        ...(options.leaseMs === undefined ? {} : { leaseMs: options.leaseMs }),
        ...(options.checkGrants === false ? {} : { grants: { keyOf: (kid) => directory.keys[kid] } })
    });
    mesh = await startMeshServer({
        nodeId: options.nodeId,
        role: 'relay',
        keys: options.keys,
        certificate: () => upstream.certificate(),
        authorityPublicKey: options.authorityPublicKey,
        // a relay signs nothing: a node below asks through it, and the answer is the authority's
        certify: (certificate) => upstream.certify(certificate),
        // and keeps nothing: what a node below sends to be saved goes on up, toward an aggregate
        persist: (address, stored, epoch, node) => upstream.persist(address, stored, epoch, node),
        load: (address, node) => upstream.load(address, node),
        upstream: relay,
        write: (address, change, access) => upstream.write(address, change, access),
        route,
        directory: () => directory,
        ...(options.host === undefined ? {} : { host: options.host }),
        port: options.meshPort ?? 0
    });
    const clients: ClientServer | undefined =
        options.clientPort === undefined
            ? undefined
            : await startClientServer({ upstream: relay, write: (address, change, access) => upstream.write(address, change, access), ...(options.accessOf === undefined ? {} : { accessOf: options.accessOf }), ...(options.host === undefined ? {} : { host: options.host }), port: options.clientPort });
    const server = mesh;
    return {
        nodeId: options.nodeId,
        meshUrl: server.url,
        clientUrl: clients?.url,
        relay,
        upstream,
        mesh: server,
        close: async () => {
            relay.close();
            for (const { link } of relays.values()) link.close();
            relays.clear();
            upstream.close();
            await server.close();
            await clients?.close();
        }
    };
}
