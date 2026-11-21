#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import type { KeyPair } from './mesh/certificates.js';
import { startRelayNode } from './mesh/RelayNode.js';
import { startHostReporting } from './hostReporting.js';

export const RELAY_HELP = `statenet-relay: a relay of the statenet's mesh, with no database

Usage: statenet-relay [options]

  --node-id <id>          This node's id, as its certificate names it       STATENET_NODE_ID
  --parents <urls>        The nodes to link up to, comma-separated, in order  STATENET_PARENTS
  --region <name>         Where it runs                                      STATENET_REGION
  --node-key <file>       Its key pair, JSON { privateKey, publicKey }       STATENET_NODE_KEY_FILE
  --certificate <file>    Its certificate from Magnetic's authority          STATENET_NODE_CERTIFICATE_FILE
  --authority-key <key>   The authority's public key (base64url)             STATENET_AUTHORITY_PUBLIC_KEY
  --authority-key-file    A file holding it (magnetic relay-certificate)     STATENET_AUTHORITY_PUBLIC_KEY_FILE
  --host <host>           The address it listens on (127.0.0.1)              STATENET_HOST
  --mesh-port <port>      The port relays below link to (4310)               STATENET_MESH_PORT
  --client-port <port>    The port browsers connect to (4311)                STATENET_CLIENT_PORT
  --help                  This text

Reports go to stderr: statenet.*, state-sync.*, info, warning and error, and the categories PUNYTS_REPORT lists
(comma-separated, such as PUNYTS_REPORT=state.set).
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
    const certificateFile = value('certificate', 'STATENET_NODE_CERTIFICATE_FILE');
    const certificate = (await readFile(certificateFile, 'utf8')).trim();
    const authorityFile = read(argv, process.env, 'authority-key-file', 'STATENET_AUTHORITY_PUBLIC_KEY_FILE');
    const authorityPublicKey = authorityFile !== undefined && authorityFile !== '' ? (await readFile(authorityFile, 'utf8')).trim() : value('authority-key', 'STATENET_AUTHORITY_PUBLIC_KEY');
    // statenet.*, state-sync.* and the operator's categories, with those PUNYTS_REPORT adds; the link up reports under statenet.mesh
    startHostReporting('statenet-relay', process.env);
    const region = read(argv, process.env, 'region', 'STATENET_REGION');
    const relay = await startRelayNode({
        nodeId: value('node-id', 'STATENET_NODE_ID'),
        ...(region === undefined ? {} : { region }),
        keys,
        certificate,
        authorityPublicKey,
        // the next certificate replaces the file, so a restart shows the newest; a file that cannot be written is said
        onCertificate: (next) => {
            void writeFile(certificateFile, `${next}\n`).then(
                () => process.stderr.write('statenet-relay: its certificate was renewed\n'),
                (error: unknown) => process.stderr.write(`statenet-relay: its certificate was renewed, but ${certificateFile} could not be written: ${error instanceof Error ? error.message : String(error)}\n`)
            );
        },
        parents: value('parents', 'STATENET_PARENTS').split(',').map((url) => url.trim()).filter((url) => url !== ''),
        host: read(argv, process.env, 'host', 'STATENET_HOST') ?? '127.0.0.1',
        meshPort: Number(read(argv, process.env, 'mesh-port', 'STATENET_MESH_PORT') ?? 4310),
        clientPort: Number(read(argv, process.env, 'client-port', 'STATENET_CLIENT_PORT') ?? 4311)
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
