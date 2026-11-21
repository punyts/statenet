import { afterEach, describe, expect, it } from 'vitest';
import { clearListeners, isReported, report, setCategories } from '@punyts/core';
import { HOST_CATEGORIES, hostCategories, startHostReporting } from './hostReporting.js';

describe('host reporting', () => {
    afterEach(() => {
        setCategories(['info', 'error', 'stack']);
        clearListeners();
    });

    it('should add the categories PUNYTS_REPORT lists, trimmed, to the families, ignoring empty ones', () => {
        // Arrange
        const env = { PUNYTS_REPORT: ' stateui-app.* , ,state.set,statenet.*' };

        // Act
        const categories = hostCategories(env);

        // Assert
        expect(categories).toEqual([...HOST_CATEGORIES, 'stateui-app.*', 'state.set']);
        expect(hostCategories({})).toEqual([...HOST_CATEGORIES]);
    });

    it('should turn the categories on and write each report to stderr as [name] category message', () => {
        // Arrange
        const lines: string[] = [];

        // Act
        startHostReporting('statenet', { PUNYTS_REPORT: 'state.set' }, (line) => lines.push(line));
        report('statenet.service', 'Loaded namespace %s at revision %s', ['apps.orders', 3]);
        report('state.set', 'Set %s', ['$.data.total']);
        report('stateui.render', 'Rendered %s', ['ui-button']);

        // Assert
        expect(lines).toEqual(['[statenet] statenet.service Loaded namespace apps.orders at revision 3\n', '[statenet] state.set Set $.data.total\n']);
        expect(isReported('state-sync.client')).toBe(true);
        expect(isReported('stateui.render')).toBe(false);
    });
});
