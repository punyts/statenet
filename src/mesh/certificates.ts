import { createPrivateKey, createPublicKey, randomBytes, sign, verify } from 'node:crypto';
import { generateGrantKey } from '@punyts/state-sync/authority';

/** A node's place in the statenet */
export type NodeRole = 'owner' | 'relay' | 'aggregate';

/** What a node certificate says: which node, in which role, with which public key, until when */
export interface NodeCertificateClaims {
    readonly nodeId: string;
    readonly role: NodeRole;
    readonly region: string;
    /** The node's own public key (SPKI DER, base64url), with which it proves it is the node in its links */
    readonly publicKey: string;
    readonly expires: number;
}

/** A key pair, each part base64url DER (PKCS#8 private, SPKI public) */
export interface KeyPair {
    readonly privateKey: string;
    readonly publicKey: string;
}

const encode = (bytes: Uint8Array | string): string => Buffer.from(bytes).toString('base64url');
const decode = (text: string): Buffer => Buffer.from(text, 'base64url');
const privateKeyOf = (key: string) => createPrivateKey({ key: decode(key), format: 'der', type: 'pkcs8' });
const publicKeyOf = (key: string) => createPublicKey({ key: decode(key), format: 'der', type: 'spki' });

/** A new Ed25519 key pair: a node's key, or Magnetic's certificate authority */
export const generateKeyPair = (): KeyPair => generateGrantKey();

/**
 * Issues a node certificate (docs/design/statenet-relays.md, "The mesh protocol"): the node's claims signed by
 * Magnetic's certificate authority key. Nodes check each other's certificates against the authority's public key when
 * a link opens
 */
export const issueNodeCertificate = (claims: NodeCertificateClaims, authorityPrivateKey: string): string => {
    const body = encode(JSON.stringify(claims));
    return `${body}.${encode(sign(null, Buffer.from(body), privateKeyOf(authorityPrivateKey)))}`;
};

/** The claims of a certificate the authority signed and that has not expired, or why not */
export const verifyNodeCertificate = (certificate: unknown, authorityPublicKey: string, now: number): { ok: true; claims: NodeCertificateClaims } | { ok: false; reason: string } => {
    if (typeof certificate !== 'string') return { ok: false, reason: 'no certificate' };
    const [body, signature, rest] = certificate.split('.');
    if (body === undefined || signature === undefined || rest !== undefined) return { ok: false, reason: 'not a certificate' };
    let signed = false;
    try {
        signed = verify(null, Buffer.from(body), publicKeyOf(authorityPublicKey), decode(signature));
    } catch {
        signed = false;
    }
    if (!signed) return { ok: false, reason: "not signed by Magnetic's certificate authority" };
    try {
        const claims = JSON.parse(decode(body).toString('utf8')) as NodeCertificateClaims;
        if (typeof claims.nodeId !== 'string' || typeof claims.publicKey !== 'string' || typeof claims.expires !== 'number') return { ok: false, reason: 'not a certificate' };
        if (claims.expires <= now) return { ok: false, reason: 'the certificate expired' };
        return { ok: true, claims };
    } catch {
        return { ok: false, reason: 'not a certificate' };
    }
};

/** A fresh challenge for the other end of a link to sign */
export const newNonce = (): string => encode(randomBytes(24));

/** Proves a node holds its key: its signature of the other end's challenge */
export const proveNode = (nonce: string, privateKey: string): string => encode(sign(null, Buffer.from(nonce), privateKeyOf(privateKey)));

/** Checks a node's proof of a challenge against the public key in its certificate */
export const checkProof = (nonce: string, proof: unknown, publicKey: string): boolean => {
    if (typeof proof !== 'string') return false;
    try {
        return verify(null, Buffer.from(nonce), publicKeyOf(publicKey), decode(proof));
    } catch {
        return false;
    }
};
