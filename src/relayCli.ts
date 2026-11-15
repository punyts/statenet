#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { addListener, setCategories } from '@punyts/core';
import type { KeyPair } from './mesh/certificates.js';
import { startRelayNode } from './mesh/RelayNode.js';

export const RELAY_HELP = `statenet-relay: a relay of the statenet's mesh, with no database

Usage: statenet-relay [options]

  --node-id <id>          This node's id, as its certificate names it       STATENET_NODE_ID
  --parents <urls>        The nodes to link up to, comma-separated, in order  STATENET_PARENTS
  --region <name>         Where it runs                                      STATENET_REGION
  --node-key <file>       Its key pair, JSON { privateKey, publicKey }       STATENET_NODE_KEY_FILE
  --certificate <file>    Its certificate from Magnetic's authority          STATENET_NODE_CERTIFICATE_FILE
  --authority-key <key>   The authority's public key (base64url)             STATENET_AUTHORITY_PUBLIC_KEY
  --host <host>           The address it listens on (127.0.0.1)              STATENET_HOST
  --mesh-port <port>      The port relays below link to (4310)               STATENET_MESH_PORT
  --client-port <port>    The port browsers connect to (4311)                STATENET_CLIENT_PORT
  --help                  This text
`;

/** Reads `--name value` flags, falling back to the environment variable */
function read(argv: readonly string[], env: NodeJS.ProcessEnv, flag: string, variable: string): string | undefined {
    const index = argv.indexOf(`--${flag}`);
    return index >= 0 ? argv[index + 1] : env[variable];
}

const main = async (): Promise<void> => {
    const argv = process.argv.slice(2);
    if (argv.includes('--help')) {
        process.stdout.write(RELAY_HELP);
        return;
    }
    const value = (flag: string, variable: string): string => {
        const found = read(argv, process.env, flag, variable);
        if (found === undefined || found === '') throw new Error(`--${flag} (or ${variable}) is required\n\n${RELAY_HELP}`);
        return found;
    };
    const keys = JSON.parse(await readFile(value('node-key', 'STATENET_NODE_KEY_FILE'), 'utf8')) as KeyPair;
    const certificate = (await readFile(value('certificate', 'STATENET_NODE_CERTIFICATE_FILE'), 'utf8')).trim();
    setCategories(['statenet', 'state-sync', 'warning', 'error']);
    addListener((_timestamp, category, message) => process.stderr.write(`[${category}] ${message}\n`));
    const region = read(argv, process.env, 'region', 'STATENET_REGION');
    const relay = await startRelayNode({
        nodeId: value('node-id', 'STATENET_NODE_ID'),
        ...(region === undefined ? {} : { region }),
        keys,
        certificate,
        authorityPublicKey: value('authority-key', 'STATENET_AUTHORITY_PUBLIC_KEY'),
        parents: value('parents', 'STATENET_PARENTS').split(',').map((url) => url.trim()).filter((url) => url !== ''),
        host: read(argv, process.env, 'host', 'STATENET_HOST') ?? '127.0.0.1',
        meshPort: Number(read(argv, process.env, 'mesh-port', 'STATENET_MESH_PORT') ?? 4310),
        clientPort: Number(read(argv, process.env, 'client-port', 'STATENET_CLIENT_PORT') ?? 4311),
        upstream: { report: (message) => process.stderr.write(`statenet-relay: ${message}\n`) }
    });
    process.stderr.write(`statenet-relay: ${relay.nodeId}, links at ${relay.meshUrl}, browsers at ${relay.clientUrl ?? '(none)'}\n`);
    let stopping = false;
    const stop = (): void => {
        if (stopping) return;
        stopping = true;
        void relay.close().finally(() => process.exit(0));
    };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);
};

main().catch((error: unknown) => {
    process.stderr.write(`statenet-relay: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
});
