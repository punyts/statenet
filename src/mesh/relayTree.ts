/**
 * The relays linked below one node, each under its parent (docs/design/statenet-relay-chains.md): the relays linked to
 * the node itself are under `root`, and the ones they passed up are under them. A relay's own link up keeps one, to
 * send them all again when the link opens again, and the aggregate keeps one, to unlist a relay with everything under it
 */
export interface RelayTree<T> {
    /** The parent a relay is listed under, or undefined when it is not */
    parentOf(id: string): string | undefined;
    get(id: string): T | undefined;
    /**
     * Lists a relay under `parent`, or moves it there with what is under it. False, and nothing changes, when the
     * relay is the root or `parent` is the relay itself or under it
     */
    link(id: string, parent: string, value: T): boolean;
    /** Takes a relay out with every relay under it; answers their ids, the relay's first */
    unlink(id: string): string[];
    /** The relay linked to the root that `id` is under (or is), or undefined when it is not listed */
    branchOf(id: string): string | undefined;
    /** Every relay with its parent and value, each after its parent */
    walk(): Array<{ readonly id: string; readonly parent: string; readonly value: T }>;
}

export function createRelayTree<T>(root: string): RelayTree<T> {
    const entries = new Map<string, { parent: string; value: T }>();
    const childrenOf = (parent: string): string[] => [...entries].filter(([, entry]) => entry.parent === parent).map(([id]) => id);
    /** Whether `id` is `ancestor` or under it */
    const within = (id: string, ancestor: string): boolean => {
        for (let at: string | undefined = id; at !== undefined && at !== root; at = entries.get(at)?.parent) if (at === ancestor) return true;
        return false;
    };
    return {
        parentOf: (id) => entries.get(id)?.parent,
        get: (id) => entries.get(id)?.value,
        link: (id, parent, value) => {
            if (id === root || within(parent, id)) return false;
            entries.set(id, { parent, value });
            return true;
        },
        unlink: (id) => {
            if (!entries.has(id)) return [];
            const removed: string[] = [];
            const queue = [id];
            for (let next = queue.shift(); next !== undefined; next = queue.shift()) {
                removed.push(next);
                queue.push(...childrenOf(next));
                entries.delete(next);
            }
            return removed;
        },
        branchOf: (id) => {
            let at: string | undefined = id;
            while (at !== undefined && entries.has(at)) {
                const parent: string = (entries.get(at) as { parent: string }).parent;
                if (parent === root) return at;
                at = parent;
            }
            return undefined;
        },
        walk: () => {
            const walked: Array<{ id: string; parent: string; value: T }> = [];
            const queue = childrenOf(root);
            for (let next = queue.shift(); next !== undefined; next = queue.shift()) {
                walked.push({ id: next, ...(entries.get(next) as { parent: string; value: T }) });
                queue.push(...childrenOf(next));
            }
            return walked;
        }
    };
}
