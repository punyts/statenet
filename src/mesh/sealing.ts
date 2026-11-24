import { createCipheriv, createDecipheriv, createPublicKey, diffieHellman, generateKeyPairSync, hkdfSync } from 'node:crypto';
import type { KeyObject } from 'node:crypto';

/**
 * The key a link's two ends agree on in the handshake (docs/design/statenet-network.md, "Authentication between
 * relays, nodes and aggregates"): each end makes an X25519 key for the one link, sends its public part (its share),
 * and signs both shares with its node key in its proof, so nobody between them can put a share of its own in. Every
 * message after the handshake is sealed with what the shares give
 */
export interface LinkShare {
    /** The public part, SPKI DER base64url: what the other end is sent */
    readonly share: string;
    readonly privateKey: KeyObject;
}

const decode = (text: string): Buffer => Buffer.from(text, 'base64url');

/** A new share for one link */
export const newShare = (): LinkShare => {
    const pair = generateKeyPairSync('x25519');
    return { share: pair.publicKey.export({ format: 'der', type: 'spki' }).toString('base64url'), privateKey: pair.privateKey };
};

/** What each end signs to prove itself: its side, the other end's challenge, both shares and both node ids */
export const transcript = (side: 'dialler' | 'server', challenge: string, link: { readonly diallerShare: string; readonly serverShare: string; readonly dialler: string; readonly server: string }): string =>
    ['statenet-link-2', side, challenge, link.diallerShare, link.serverShare, link.dialler, link.server].join('|');

/** One end of a sealed link: `seal` the bytes it sends, `open` what it gets */
export interface LinkSeal {
    seal(bytes: Uint8Array): Buffer;
    /** The bytes, or undefined when they were changed, replayed, dropped before or out of order: the link must close */
    open(data: unknown): Buffer | undefined;
}

const TAG = 16;

const bytesOf = (data: unknown): Buffer | undefined => {
    if (Buffer.isBuffer(data)) return data;
    if (Array.isArray(data)) return Buffer.concat(data as Buffer[]);
    if (data instanceof ArrayBuffer) return Buffer.from(data);
    return undefined;
};

/**
 * The seal of one end of a link: AES-256-GCM under a key per direction, both from the shares and the two challenges.
 * Each message's nonce is its number on the link, counted by both ends and never sent, so a message that is replayed,
 * left out or moved does not open. Undefined when the other end's share is not a key
 */
export function sealOf(mine: LinkShare, theirs: string, challenges: { readonly dialler: string; readonly server: string }, side: 'dialler' | 'server'): LinkSeal | undefined {
    let material: Buffer;
    try {
        const theirKey = createPublicKey({ key: decode(theirs), format: 'der', type: 'spki' });
        if (theirKey.asymmetricKeyType !== 'x25519') return undefined;
        const secret = diffieHellman({ privateKey: mine.privateKey, publicKey: theirKey });
        material = Buffer.from(hkdfSync('sha256', secret, Buffer.from(`${challenges.dialler}|${challenges.server}`), Buffer.from('statenet-link-2'), 64));
    } catch {
        return undefined;
    }
    const up = material.subarray(0, 32);
    const down = material.subarray(32, 64);
    const sendKey = side === 'dialler' ? up : down;
    const receiveKey = side === 'dialler' ? down : up;
    let sent = 0n;
    let received = 0n;
    const nonce = (count: bigint): Buffer => {
        const bytes = Buffer.alloc(12);
        bytes.writeBigUInt64BE(count, 4);
        return bytes;
    };
    return {
        seal: (bytes) => {
            const cipher = createCipheriv('aes-256-gcm', sendKey, nonce(sent++));
            return Buffer.concat([cipher.update(bytes), cipher.final(), cipher.getAuthTag()]);
        },
        open: (data) => {
            const bytes = bytesOf(data);
            if (bytes === undefined || bytes.length < TAG) return undefined;
            try {
                const decipher = createDecipheriv('aes-256-gcm', receiveKey, nonce(received));
                decipher.setAuthTag(bytes.subarray(bytes.length - TAG));
                const opened = Buffer.concat([decipher.update(bytes.subarray(0, bytes.length - TAG)), decipher.final()]);
                received++;
                return opened;
            } catch {
                return undefined;
            }
        }
    };
}
