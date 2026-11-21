import { addListener, setCategories } from '@punyts/core';

/** What statenet's bins report by default: their own and state-sync's families, and the operator's built-in categories */
export const HOST_CATEGORIES: readonly string[] = ['statenet.*', 'state-sync.*', 'info', 'warning', 'error'];

/**
 * The categories a bin turns on: `HOST_CATEGORIES` and those `PUNYTS_REPORT` names, a comma-separated list
 * (`PUNYTS_REPORT=stateui-app.*,state.set`), each trimmed, empty ones ignored
 * @param env
 */
export function hostCategories(env: Readonly<Record<string, string | undefined>>): string[] {
    const extra = (env['PUNYTS_REPORT'] ?? '')
        .split(',')
        .map((category) => category.trim())
        .filter((category) => category !== '');
    return [...new Set([...HOST_CATEGORIES, ...extra])];
}

/**
 * Turns on a bin's categories and writes every report to stderr as `[<name>] <category> <message>`; the reporter has
 * filled the message from its details already
 * @param name - the bin, `statenet` or `statenet-relay`
 * @param env
 * @param write - stderr by default
 */
export function startHostReporting(name: string, env: Readonly<Record<string, string | undefined>>, write: (line: string) => void = (line) => process.stderr.write(line)): string[] {
    const categories = hostCategories(env);
    setCategories(categories);
    addListener((_timestamp, category, message) => write(`[${name}] ${category} ${message}\n`), categories);
    return categories;
}
