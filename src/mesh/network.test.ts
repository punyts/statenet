import type { Upstream } from '@punyts/state-sync/authority';
import { addListener, removeListener, setCategories } from '@punyts/core';
import type { ListenerFn } from '@punyts/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import { generateKeyPair, issueNodeCertificate, mayLink, proveNode, renewNodeCertificate, verifyNodeCertificate } from './certificates.js';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { startMeshServer } from './MeshServer.js';
import type { MeshServerOptions } from './MeshServer.js';
import { createMeshUpstream } from './MeshUpstream.js';
import type { MeshUpstreamOptions } from './MeshUpstream.js';
import { MESH_PROTOCOL } from './protocol.js';
import type { DirectoryView, LinkMessage } from './protocol.js';
import { startRelayNode } from './RelayNode.js';
import { newShare, sealOf, transcript } from './sealing.js';
import type { LinkSeal } from './sealing.js';

const HOUR = 3_600_000;
const authority = generateKeyPair();
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

const certificateFor = (nodeId: string, role: NodeRole, keys: KeyPair, life = HOUR): string =>
    issueNodeCertificate({ nodeId, role, region: '', publicKey: keys.publicKey, expires: Date.now() + life }, authority.privateKey);

const nothing: Upstream = { subscribe: () => ({ renew: () => undefined, close: () => undefined }) };
const EMPTY: DirectoryView = { nodes: [], owners: {}, keys: {} };

/** A mesh server of a role that serves nothing: what the tests look at is who it lets link */
async function server(nodeId: string, role: NodeRole, more: Partial<MeshServerOptions> = {}) {
    const keys = generateKeyPair();
    const mesh = await startMeshServer({
        nodeId,
        role,
        keys,
        certificate: certificateFor(nodeId, role, keys),
        authorityPublicKey: authority.publicKey,
        upstream: nothing,
        write: async () => ({ result: 'rejected', reason: 'nothing is written here' }),
        directory: () => EMPTY,
        ...more
    });
    closers.push(() => mesh.close());
    return { mesh, keys };
}

/** A node's link up, with what it reported */
function dial(nodeId: string, role: NodeRole, url: string, more: Partial<MeshUpstreamOptions> = {}) {
    const keys = more.keys ?? generateKeyPair();
    const upstream = createMeshUpstream({
        nodeId,
        role,
        keys,
        certificate: certificateFor(nodeId, role, keys),
        authorityPublicKey: authority.publicKey,
        parents: [url],
        followDirectory: false,
        backoff: () => 20,
        ...more
    });
    closers.push(() => upstream.close());
    return { upstream, keys, get said() { return heardOf(nodeId); } };
}

/** A link opened by hand over a bare socket: the handshake, then the seal and the socket to misuse */
async function handshake(url: string, nodeId: string, role: NodeRole): Promise<{ socket: WebSocket; seal: LinkSeal; closed: Promise<number> }> {
    const keys = generateKeyPair();
    const nonce = 'challenge-of-the-test';
    const mine = newShare();
    const socket = new WebSocket(url);
    closers.push(() => socket.terminate());
    const closed = new Promise<number>((resolve) => socket.on('close', (code) => resolve(code)));
    await new Promise((resolve) => socket.on('open', resolve));
    const welcome = new Promise<Extract<LinkMessage, { type: 'link-welcome' }>>((resolve) => socket.once('message', (data) => resolve(JSON.parse(String(data)))));
    socket.send(JSON.stringify({ type: 'link-hello', nodeId, role, protocol: MESH_PROTOCOL, region: '', certificate: certificateFor(nodeId, role, keys), nonce, share: mine.share }));
    const welcomed = await welcome;
    const facts = { diallerShare: mine.share, serverShare: welcomed.share, dialler: nodeId, server: welcomed.nodeId };
    const seal = sealOf(mine, welcomed.share, { dialler: nonce, server: welcomed.nonce }, 'dialler') as LinkSeal;
    const first = new Promise((resolve) => socket.once('message', resolve));
    socket.send(JSON.stringify({ type: 'link-proof', proof: proveNode(transcript('dialler', welcomed.nonce, facts), keys.privateKey) }));
    // the server's first sealed frame, the directory: it must be opened to keep the count of messages
    seal.open(await first);
    return { socket, seal, closed };
}

/** The warnings and statenet reports heard during a test, from @punyts/core's reporter */
const heard: string[] = [];
const hear: ListenerFn = (_timestamp, _category, message) => {
    heard.push(message);
};
const HEARD = ['warning', 'statenet.*'];
beforeEach(() => {
    heard.length = 0;
    setCategories(['error', ...HEARD]);
    addListener(hear, HEARD);
});
afterEach(() => {
    removeListener(hear, HEARD);
    setCategories(['info', 'error', 'stack']);
});
/** What was heard of a node: the reports that name it */
const heardOf = (nodeId: string): string[] => heard.filter((line) => line.includes(nodeId));

describe('who may link to whom', () => {
    it('should let a node dial a node or a relay, a relay dial a node, a relay or an aggregate, and an aggregate dial nothing', () => {
        // Arrange
        const roles: NodeRole[] = ['owner', 'relay', 'aggregate'];

        // Act
        const allowed = roles.flatMap((dialler) => roles.filter((to) => mayLink(dialler, to)).map((to) => `${dialler}>${to}`));

        // Assert
        expect(allowed).toEqual(['owner>owner', 'owner>relay', 'relay>owner', 'relay>relay', 'relay>aggregate']);
    });

    it('should refuse a certified node whose role does not link to the server, and say why', async () => {
        // Arrange
        const aggregate = await server('aggregate-1', 'aggregate');

        // Act
        const node = dial('node-1', 'owner', aggregate.mesh.url);
        await vi.waitFor(() => expect(node.said.some((line) => line.includes('refused'))).toBe(true));

        // Assert
        expect(node.upstream.linked()).toBe(false);
        expect(node.said.join('\n')).toContain('a node of role owner does not link to one of role aggregate');
        expect(aggregate.mesh.stats().links).toEqual([]);
    });

    it('should link a relay to an aggregate', async () => {
        // Arrange
        const aggregate = await server('aggregate-1', 'aggregate');

        // Act
        const relay = dial('relay-1', 'relay', aggregate.mesh.url);

        // Assert
        await vi.waitFor(() => expect(relay.upstream.linked()).toBe(true));
        expect(aggregate.mesh.stats().links.map((link) => link.nodeId)).toEqual(['relay-1']);
    });

    it('should refuse a hello that names another role than its certificate', async () => {
        // Arrange: a relay's certificate, said to be a node's
        const node = await server('node-1', 'owner');
        const keys = generateKeyPair();
        const socket = new WebSocket(node.mesh.url);
        closers.push(() => socket.terminate());
        await new Promise((resolve) => socket.on('open', resolve));
        const answer = new Promise<LinkMessage>((resolve) => socket.once('message', (data) => resolve(JSON.parse(String(data)))));

        // Act
        socket.send(JSON.stringify({ type: 'link-hello', nodeId: 'relay-1', role: 'owner', protocol: MESH_PROTOCOL, region: '', certificate: certificateFor('relay-1', 'relay', keys), nonce: 'n', share: newShare().share }));

        // Assert
        expect(await answer).toEqual({ type: 'link-refused', reason: 'the certificate is for another role' });
    });

    it('should not link an aggregate that dials out', async () => {
        // Arrange
        const node = await server('node-1', 'owner');

        // Act
        const aggregate = dial('aggregate-1', 'aggregate', node.mesh.url);
        await vi.waitFor(() => expect(aggregate.said.join(' ')).toContain('refused'));

        // Assert
        expect(aggregate.upstream.linked()).toBe(false);
    });
});

describe('nodes that must be in the directory', () => {
    const listing = (nodes: DirectoryView['nodes']): DirectoryView => ({ nodes, owners: {}, keys: {} });

    it('should refuse a certified node the directory does not list, or lists with another key, and link the one it lists', async () => {
        // Arrange
        const listedKeys = generateKeyPair();
        const directory = listing([
            { id: 'node-listed', role: 'owner', region: '', endpoint: '', publicKey: listedKeys.publicKey },
            { id: 'node-other-key', role: 'owner', region: '', endpoint: '', publicKey: generateKeyPair().publicKey }
        ]);
        const parent = await server('node-1', 'owner', { requireListed: ['owner'], directory: () => directory });

        // Act
        const unlisted = dial('node-unlisted', 'owner', parent.mesh.url);
        const otherKey = dial('node-other-key', 'owner', parent.mesh.url);
        const listed = dial('node-listed', 'owner', parent.mesh.url, { keys: listedKeys });

        // Assert
        await vi.waitFor(() => expect(listed.upstream.linked()).toBe(true));
        await vi.waitFor(() => expect(unlisted.said.join('\n')).toContain('the node is not in the directory'));
        await vi.waitFor(() => expect(otherKey.said.join('\n')).toContain('the node is not in the directory'));
        expect(parent.mesh.stats().links.map((link) => link.nodeId)).toEqual(['node-listed']);
    });

    it('should link a relay by its certificate alone when only nodes must be listed', async () => {
        // Arrange
        const parent = await server('node-1', 'owner', { requireListed: ['owner'] });

        // Act
        const relay = dial('relay-1', 'relay', parent.mesh.url);

        // Assert
        await vi.waitFor(() => expect(relay.upstream.linked()).toBe(true));
    });

    it('should cut off a node taken out of the directory, and not let it back', async () => {
        // Arrange
        const keys = generateKeyPair();
        let directory = listing([{ id: 'node-2', role: 'owner', region: '', endpoint: '', publicKey: keys.publicKey }]);
        const parent = await server('node-1', 'owner', { requireListed: ['owner'], directory: () => directory });
        const node = dial('node-2', 'owner', parent.mesh.url, { keys });
        await vi.waitFor(() => expect(node.upstream.linked()).toBe(true));

        // Act
        directory = listing([]);
        parent.mesh.directoryChanged();

        // Assert
        await vi.waitFor(() => expect(node.upstream.linked()).toBe(false));
        await vi.waitFor(() => expect(node.said.join('\n')).toContain('the node is not in the directory'));
        expect(parent.mesh.stats().links).toEqual([]);
    });
});

describe('sealed links', () => {
    it('should open what the other end sealed, in order, and nothing changed, replayed or out of order', () => {
        // Arrange
        const dialler = newShare();
        const parent = newShare();
        const challenges = { dialler: 'a', server: 'b' };
        const down = sealOf(dialler, parent.share, challenges, 'dialler') as LinkSeal;
        const up = sealOf(parent, dialler.share, challenges, 'server') as LinkSeal;
        const again = sealOf(parent, dialler.share, challenges, 'server') as LinkSeal;

        // Act
        const first = down.seal('one');
        const second = down.seal('two');
        const changed = Buffer.from(first);
        changed[0] = (changed[0] as number) ^ 1;

        // Assert
        expect(first.toString('utf8')).not.toContain('one');
        expect(up.open(changed)).toBeUndefined();
        expect(up.open(first)).toBe('one');
        expect(up.open(first)).toBeUndefined();
        expect(up.open(second)).toBe('two');
        expect(again.open(second)).toBeUndefined();
        expect(down.open(first)).toBeUndefined();
        expect(sealOf(dialler, 'not-a-key', challenges, 'dialler')).toBeUndefined();
    });

    it('should seal with another key when someone between the ends puts in a share of its own', () => {
        // Arrange
        const dialler = newShare();
        const parent = newShare();
        const between = newShare();
        const challenges = { dialler: 'a', server: 'b' };

        // Act
        const toParent = sealOf(dialler, between.share, challenges, 'dialler') as LinkSeal;
        const atParent = sealOf(parent, dialler.share, challenges, 'server') as LinkSeal;

        // Assert: and the proofs sign the shares each end saw, so they do not check either
        expect(atParent.open(toParent.seal('secret'))).toBeUndefined();
        const seenByDialler = { diallerShare: dialler.share, serverShare: between.share, dialler: 'd', server: 's' };
        const seenByParent = { diallerShare: dialler.share, serverShare: parent.share, dialler: 'd', server: 's' };
        expect(transcript('server', 'a', seenByDialler)).not.toBe(transcript('server', 'a', seenByParent));
    });

    it('should close a link on a message in the clear after its handshake', async () => {
        // Arrange
        const parent = await server('node-1', 'owner');
        const link = await handshake(parent.mesh.url, 'relay-1', 'relay');
        expect(parent.mesh.stats().links.map((each) => each.nodeId)).toEqual(['relay-1']);

        // Act
        link.socket.send(JSON.stringify({ type: 'frame', messages: [{ type: 'credit', amount: 10 }] }));

        // Assert
        expect(await link.closed).toBe(4400);
        expect(parent.mesh.stats().links).toEqual([]);
    });

    it('should take a sealed frame once, and close the link when it is sent again', async () => {
        // Arrange
        const parent = await server('node-1', 'owner');
        const link = await handshake(parent.mesh.url, 'relay-1', 'relay');
        const frame = link.seal.seal(JSON.stringify({ type: 'frame', messages: [{ type: 'credit', amount: 10 }] }));

        // Act
        link.socket.send(frame);
        await vi.waitFor(() => expect(parent.mesh.stats().links[0]?.credit).toBe(10));
        link.socket.send(frame);

        // Assert
        expect(await link.closed).toBe(4400);
    });
});

describe('the next certificate over the link', () => {
    const reissue = (seen: NodeCertificateClaims[] = []): NonNullable<MeshServerOptions['certify']> => async (certificate, claims) => {
        seen.push(claims);
        return renewNodeCertificate(certificate, authority, Date.now() + 24 * HOUR, Date.now());
    };
    const expiryOf = (certificate: string): number => {
        const checked = verifyNodeCertificate(certificate, authority.publicKey, Date.now());
        return checked.ok ? checked.claims.expires : 0;
    };

    it('should give a node whose certificate ends soon its next one, with the same node, role and key', async () => {
        // Arrange
        const parent = await server('node-1', 'owner', { certify: reissue() });
        const got: string[] = [];

        // Act: an hour is left, and it asks when less than two are
        const node = dial('node-2', 'owner', parent.mesh.url, { renewWithinMs: 2 * HOUR, onCertificate: (next) => got.push(next) });
        const before = node.upstream.certificate();

        // Assert
        await vi.waitFor(() => expect(got).toHaveLength(1));
        const checked = verifyNodeCertificate(got[0], authority.publicKey, Date.now());
        expect(checked.ok && checked.claims).toMatchObject({ nodeId: 'node-2', role: 'owner', publicKey: node.keys.publicKey });
        expect(expiryOf(got[0] as string)).toBeGreaterThan(expiryOf(before));
        expect(node.upstream.certificate()).toBe(got[0]);
    });

    it('should not ask while its certificate has long to go, nor get one from a parent that renews nothing', async () => {
        // Arrange
        const asked: NodeCertificateClaims[] = [];
        const renewing = await server('node-1', 'owner', { certify: reissue(asked) });
        const silent = await server('node-3', 'owner');

        // Act
        const patient = dial('node-2', 'owner', renewing.mesh.url, { renewWithinMs: HOUR / 2 });
        const unanswered = dial('node-4', 'owner', silent.mesh.url, { renewWithinMs: 2 * HOUR });
        const before = unanswered.upstream.certificate();
        await vi.waitFor(() => expect(patient.upstream.linked() && unanswered.upstream.linked()).toBe(true));
        const answer = await unanswered.upstream.certify(before);

        // Assert
        expect(asked).toEqual([]);
        expect(answer).toBeUndefined();
        expect(unanswered.upstream.certificate()).toBe(before);
    });

    it('should refuse a node that asks for another node\'s certificate, and pass on what a relay asks for the node below it', async () => {
        // Arrange
        const asked: NodeCertificateClaims[] = [];
        const top = await server('node-1', 'owner', { certify: reissue(asked) });
        const relayKeys = generateKeyPair();
        const relay = await startRelayNode({ nodeId: 'relay-1', keys: relayKeys, certificate: certificateFor('relay-1', 'relay', relayKeys), authorityPublicKey: authority.publicKey, parents: [top.mesh.url], checkGrants: false, upstream: { renewWithinMs: 0 } });
        closers.push(() => relay.close());
        // none of them asks by itself: only what the test asks reaches the top
        const below = dial('relay-2', 'relay', relay.meshUrl, { renewWithinMs: 0 });
        const beside = dial('node-2', 'owner', top.mesh.url, { renewWithinMs: 0 });
        await vi.waitFor(() => expect(below.upstream.linked() && beside.upstream.linked() && relay.upstream.linked()).toBe(true));
        const other = certificateFor('node-9', 'owner', generateKeyPair());

        // Act
        const refused = await beside.upstream.certify(other);
        const passedOn = await below.upstream.certify(below.upstream.certificate());

        // Assert
        expect(refused).toBeUndefined();
        expect(asked.map((claims) => claims.nodeId)).toEqual(['relay-2']);
        expect(expiryOf(passedOn as string)).toBeGreaterThan(expiryOf(below.upstream.certificate()));
    });

    it('should not renew a certificate another authority signed, or one that has ended', () => {
        // Arrange
        const keys = generateKeyPair();
        const stranger = generateKeyPair();
        const theirs = issueNodeCertificate({ nodeId: 'n', role: 'relay', region: '', publicKey: keys.publicKey, expires: Date.now() + HOUR }, stranger.privateKey);
        const ended = issueNodeCertificate({ nodeId: 'n', role: 'relay', region: '', publicKey: keys.publicKey, expires: Date.now() - 1 }, authority.privateKey);

        // Act
        const results = [theirs, ended].map((certificate) => renewNodeCertificate(certificate, authority, Date.now() + HOUR, Date.now()));

        // Assert
        expect(results).toEqual([undefined, undefined]);
    });
});
