import { createRelay } from '@punyts/state-sync/authority';
import type { Relay } from '@punyts/state-sync/authority';
import type { KeyPair } from './certificates.js';
import { startClientServer } from './ClientServer.js';
import type { ClientServer, ClientServerOptions } from './ClientServer.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServer } from './MeshServer.js';
import { createMeshUpstream } from './MeshUpstream.js';
import type { MeshUpstream, MeshUpstreamOptions } from './MeshUpstream.js';
import type { DirectoryView } from './protocol.js';

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
 * from its parent's directory, and it checks reads with those keys at its edge
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
        upstream: relay,
        write: (address, change, access) => upstream.write(address, change, access),
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
            upstream.close();
            await server.close();
            await clients?.close();
        }
    };
}
