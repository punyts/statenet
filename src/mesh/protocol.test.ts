import { encodeCbor } from '@punyts/state-sync';
import { describe, expect, it } from 'vitest';
import { decodeLinkMessage, encodeLinkMessage } from './protocol.js';
import type { LinkMessage } from './protocol.js';

describe('the messages of an open link', () => {
    it('should carry a frame through CBOR, a say\'s data as text or bytes, and leave out members that are undefined', () => {
        // Arrange
        const bytes = new Uint8Array([0x83, 0x03, 0xff]);
        const frame: LinkMessage = {
            type: 'frame',
            messages: [
                { type: 'say', tid: 1, data: '{"type":"hello"}' },
                { type: 'say', tid: 1, data: bytes },
                { type: 'rev', message: { type: 'say', tid: 2, data: bytes } },
                { type: 'subscribe', sid: 3, address: '1.notes', prefix: '$.data', revision: undefined }
            ]
        };

        // Act
        const read = decodeLinkMessage(encodeLinkMessage(frame));

        // Assert
        expect(read).toEqual({
            type: 'frame',
            messages: [
                { type: 'say', tid: 1, data: '{"type":"hello"}' },
                { type: 'say', tid: 1, data: bytes },
                { type: 'rev', message: { type: 'say', tid: 2, data: bytes } },
                { type: 'subscribe', sid: 3, address: '1.notes', prefix: '$.data' }
            ]
        });
        expect(Object.keys((read as Extract<LinkMessage, { type: 'frame' }>).messages[3] as object)).not.toContain('revision');
    });

    it('should leave out of a frame a message it cannot encode, and name its type', () => {
        // Arrange
        const dropped: string[] = [];
        const frame = { type: 'frame', messages: [{ type: 'credit', amount: 1 }, { type: 'durable', sid: 1, revision: Number.NaN }, { type: 'credit', amount: 2 }] } as LinkMessage;

        // Act
        const read = decodeLinkMessage(encodeLinkMessage(frame, (type) => dropped.push(type)));

        // Assert
        expect(read).toEqual({ type: 'frame', messages: [{ type: 'credit', amount: 1 }, { type: 'credit', amount: 2 }] });
        expect(dropped).toEqual(['durable']);
    });

    it('should refuse bytes that are not CBOR, a value with no type, and bytes anywhere but a say\'s data', () => {
        // Arrange
        const write = (value: unknown) => encodeCbor(value as never, { bytes: true });

        // Act
        const refused = [
            new TextEncoder().encode(JSON.stringify({ type: 'ping' })),
            write({ messages: [] }),
            write({ type: 'frame', messages: [{ type: 'say', tid: 1, data: 7 }] }),
            write({ type: 'frame', messages: [{ type: 'say', tid: new Uint8Array([1]), data: 'x' }] }),
            write({ type: 'frame', messages: [{ type: 'rev', message: { type: 'credit', amount: new Uint8Array([1]) } }] }),
            write({ type: 'ping', extra: new Uint8Array([1]) })
        ].map(decodeLinkMessage);

        // Assert
        expect(refused).toEqual([undefined, undefined, undefined, undefined, undefined, undefined]);
        expect(decodeLinkMessage(write({ type: 'pong' }))).toEqual({ type: 'pong' });
    });
});
