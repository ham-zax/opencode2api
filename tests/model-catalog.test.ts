import nativeCapture from './fixtures/opencode2-zen.json';
import { describe, test, expect } from 'bun:test';
import { buildZenCatalog, resolveCatalogModel } from '../model-catalog';

// Independent literal fixtures (not derived from the implementation).

function freeMeta(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    cost: { input: 0, output: 0 },
    modalities: { input: ['text'], output: ['text'] },
    ...overrides,
  };
}

function upstreamModel(id: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { id, object: 'model', created: 1700000000, owned_by: 'opencode', ...extra };
}

function doc(models: Record<string, unknown>, opencodeExtra: Record<string, unknown> = {}): unknown {
  return { opencode: { models, ...opencodeExtra } };
}

function deepFreeze(value: unknown): void {
  if (value && typeof value === 'object') {
    if (Array.isArray(value)) value.forEach(deepFreeze);
    else for (const v of Object.values(value as Record<string, unknown>)) deepFreeze(v);
    Object.freeze(value);
  }
}

describe('buildZenCatalog validation', () => {
  const goodMeta = () => doc({ 'a-free': freeMeta() });

  test('empty upstream array is a valid empty catalog', () => {
    expect(buildZenCatalog({ data: [] }, goodMeta())).toEqual({ models: [], excluded: [] });
  });

  test.each([
    ['null upstream', null, goodMeta()],
    ['missing data', {}, goodMeta()],
    ['non-array data', { data: {} }, goodMeta()],
    ['array upstream', [], goodMeta()],
    ['null metadata', { data: [] }, null],
    ['missing opencode', { data: [] }, {}],
    ['missing models', { data: [] }, { opencode: {} }],
    ['empty registry', { data: [] }, doc({})],
    ['array registry', { data: [] }, { opencode: { models: [] } }],
    ['go-only registry', { data: [] }, { 'opencode-go': { models: { 'x-free': freeMeta() } } }],
  ])('malformed schema throws: %s', (_label, upstream, metadata) => {
    expect(() => buildZenCatalog(upstream, metadata)).toThrow();
  });
});

describe('free-price intersection', () => {
  test('selects exact-zero-cost models with default chat endpoint', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('plain-model'), upstreamModel('paid-model')] },
      doc({ 'plain-model': freeMeta(), 'paid-model': freeMeta({ cost: { input: 0, output: 1 } }) }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['plain-model']);
    expect(catalog.models[0].endpoint).toBe('chat');
    expect(catalog.excluded).toHaveLength(1);
    expect(catalog.excluded[0].id).toBe('paid-model');
    expect(catalog.excluded[0].reason).toMatch(/free|cost|price/i);
  });

  test('no suffix inference: suffix-free but priced id excluded, unsuffixed free id included', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('pricey-free'), upstreamModel('big-pickle')] },
      doc({
        'pricey-free': freeMeta({ cost: { input: 0, output: 5 } }),
        'big-pickle': freeMeta(),
      }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['big-pickle']);
    expect(catalog.excluded.map((e) => e.id)).toEqual(['pricey-free']);
  });

  test('string zero is not numeric zero', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('str-zero')] },
      doc({ 'str-zero': freeMeta({ cost: { input: '0', output: 0 } }) }),
    );
    expect(catalog.models).toEqual([]);
    expect(catalog.excluded).toHaveLength(1);
  });

  test('missing cost excluded; zero cache costs included', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('no-cost'), upstreamModel('cached')] },
      doc({
        'no-cost': { modalities: { input: ['text'], output: ['text'] } },
        cached: freeMeta({ cost: { input: 0, output: 0, cache_read: 0, cache_write: 0 } }),
      }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['cached']);
  });

  test('nonzero cache_read/cache_write excluded despite zero input/output', () => {
    const catalog = buildZenCatalog(
      {
        data: [upstreamModel('read-fee'), upstreamModel('write-fee')],
      },
      doc({
        'read-fee': freeMeta({ cost: { input: 0, output: 0, cache_read: 1 } }),
        'write-fee': freeMeta({ cost: { input: 0, output: 0, cache_write: 2 } }),
      }),
    );
    expect(catalog.models).toEqual([]);
    expect(catalog.excluded).toHaveLength(2);
  });

  test('price change to nonzero drops a previously free model', () => {
    const upstream = { data: [upstreamModel('flip')] };
    const before = buildZenCatalog(upstream, doc({ flip: freeMeta() }));
    expect(before.models.map((m) => m.id)).toEqual(['flip']);
    const after = buildZenCatalog(upstream, doc({ flip: freeMeta({ cost: { input: 0.5, output: 0 } }) }));
    expect(after.models).toEqual([]);
    expect(after.excluded.map((e) => e.id)).toEqual(['flip']);
  });

  test('opencode-go namespace never leaks in', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('go-only'), upstreamModel('zen-only')] },
      {
        'opencode-go': { models: { 'go-only': freeMeta() } },
        opencode: { models: { 'zen-only': freeMeta() } },
      },
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['zen-only']);
    expect(catalog.excluded.map((e) => e.id)).toEqual(['go-only']);
  });
});

describe('status, family, and modalities exclusions', () => {
  test('deprecated entries excluded even though upstream advertises them free', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('old-free')] },
      doc({ 'old-free': freeMeta({ status: 'deprecated' }) }),
    );
    expect(catalog.models).toEqual([]);
    expect(catalog.excluded[0].reason).toMatch(/deprecat/i);
  });

  test('jev family excluded in any case; near-miss prefix still eligible', () => {
    const catalog = buildZenCatalog(
      {
        data: [upstreamModel('jev-2-think'), upstreamModel('JEV'), upstreamModel('jevx-free')],
      },
      doc({
        'jev-2-think': freeMeta(),
        JEV: freeMeta(),
        'jevx-free': freeMeta(),
      }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['jevx-free']);
    expect(catalog.excluded.map((e) => e.id).sort()).toEqual(['JEV', 'jev-2-think']);
  });

  test('missing metadata and non-text output excluded without speculation', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('ghost'), upstreamModel('painter'), upstreamModel('mute')] },
      doc({
        painter: freeMeta({ modalities: { input: ['text'], output: ['image'] } }),
        mute: freeMeta({ modalities: { input: ['text'] } }),
      }),
    );
    expect(catalog.models).toEqual([]);
    expect(catalog.excluded.map((e) => e.id).sort()).toEqual(['ghost', 'mute', 'painter']);
  });

  test('multi-output including text is accepted', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('multi')] },
      doc({ multi: freeMeta({ modalities: { input: ['text'], output: ['text', 'image'] } }) }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual(['multi']);
  });
});

describe('transport inference', () => {
  test('future responses-only version routes via entry provider npm', () => {
    const id = 'muse-spark-9.9-contributor-free';
    const catalog = buildZenCatalog(
      { data: [upstreamModel(id)] },
      doc({ [id]: freeMeta({ provider: { npm: '@ai-sdk/openai' } }) }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual([id, 'muse-spark-9.9-free']);
    expect(catalog.models[0].endpoint).toBe('responses');
    expect(catalog.models[1].endpoint).toBe('responses');
    expect(catalog.models[1].canonical_id).toBe(id);
  });

  test('provider-level npm default applies when entry has none', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('prov-default')] },
      doc({ 'prov-default': freeMeta() }, { npm: '@ai-sdk/openai' }),
    );
    expect(catalog.models[0].endpoint).toBe('responses');
  });

  test('entry provider npm wins over provider-level npm', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('override')] },
      doc(
        { override: freeMeta({ provider: { npm: '@ai-sdk/openai-compatible' } }) },
        { npm: '@ai-sdk/openai' },
      ),
    );
    expect(catalog.models[0].endpoint).toBe('chat');
  });

  test('unsupported SDK excluded', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('exotic')] },
      doc({ exotic: freeMeta({ provider: { npm: '@ai-sdk/anthropic' } }) }),
    );
    expect(catalog.models).toEqual([]);
    expect(catalog.excluded[0].reason).toMatch(/transport|sdk|unsupported/i);
  });
});

describe('aliases', () => {
  test('contributor-free gains a -free alias preserving fields', () => {
    const id = 'muse-spark-7.1-contributor-free';
    const catalog = buildZenCatalog(
      { data: [upstreamModel(id, { created: 123, owned_by: 'zen' })] },
      doc({ [id]: freeMeta() }),
    );
    expect(catalog.models).toHaveLength(2);
    const [canonical, alias] = catalog.models;
    expect(canonical.canonical_id).toBeUndefined();
    expect(alias.id).toBe('muse-spark-7.1-free');
    expect(alias.canonical_id).toBe(id);
    expect(alias.created).toBe(123);
    expect(alias.owned_by).toBe('zen');
    expect(alias.endpoint).toBe(canonical.endpoint);
  });

  test('no alias when the -free id is already an actual selected model', () => {
    const canonical = 'muse-spark-7.1-contributor-free';
    const taken = 'muse-spark-7.1-free';
    const catalog = buildZenCatalog(
      { data: [upstreamModel(canonical), upstreamModel(taken)] },
      doc({ [canonical]: freeMeta(), [taken]: freeMeta() }),
    );
    expect(catalog.models.map((m) => m.id)).toEqual([canonical, taken]);
    expect(catalog.models.every((m) => m.canonical_id === undefined)).toBe(true);
  });
});

describe('deduplication, preservation, non-mutation', () => {
  test('duplicate upstream ids collapse to the first entry', () => {
    const catalog = buildZenCatalog(
      { data: [upstreamModel('dup', { created: 1 }), upstreamModel('dup', { created: 2 })] },
      doc({ dup: freeMeta() }),
    );
    expect(catalog.models).toHaveLength(1);
    expect(catalog.models[0].created).toBe(1);
    expect(catalog.excluded).toEqual([]);
  });

  test('inputs are not mutated', () => {
    const upstream = { data: [upstreamModel('muse-spark-7.1-contributor-free', { created: 5 })] };
    const metadata = doc({ 'muse-spark-7.1-contributor-free': freeMeta() });
    const beforeUpstream = JSON.stringify(upstream);
    const beforeMeta = JSON.stringify(metadata);
    deepFreeze(upstream);
    deepFreeze(metadata);
    const catalog = buildZenCatalog(upstream, metadata);
    expect(catalog.models).toHaveLength(2);
    expect(JSON.stringify(upstream)).toBe(beforeUpstream);
    expect(JSON.stringify(metadata)).toBe(beforeMeta);
  });
});

describe('resolveCatalogModel', () => {
  const catalog = buildZenCatalog(
    {
      data: [
        upstreamModel('muse-spark-7.1-contributor-free'),
        upstreamModel('mimo-v2.5-free'),
        upstreamModel('big-pickle'),
      ],
    },
    doc({
      'muse-spark-7.1-contributor-free': freeMeta({ provider: { npm: '@ai-sdk/openai' } }),
      'mimo-v2.5-free': freeMeta(),
      'big-pickle': freeMeta(),
    }),
  );
  const models = catalog.models;

  test('exact ids resolve, including generated aliases', () => {
    expect(resolveCatalogModel('big-pickle', models)?.id).toBe('big-pickle');
    expect(resolveCatalogModel('muse-spark-7.1-free', models)?.canonical_id).toBe(
      'muse-spark-7.1-contributor-free',
    );
  });

  test('alias counterpart resolves when the alias entry is absent', () => {
    const withoutAlias = models.filter((m) => m.id !== 'muse-spark-7.1-free');
    expect(resolveCatalogModel('muse-spark-7.1-free', withoutAlias)?.id).toBe(
      'muse-spark-7.1-contributor-free',
    );
    const withoutCanonical = models.filter((m) => m.id !== 'muse-spark-7.1-contributor-free');
    expect(resolveCatalogModel('muse-spark-7.1-contributor-free', withoutCanonical)?.id).toBe(
      'muse-spark-7.1-free',
    );
  });

  test('stripped ids resolve: generic and muse-version forms', () => {
    expect(resolveCatalogModel('mimo-v2.5', models)?.id).toBe('mimo-v2.5-free');
    expect(resolveCatalogModel('muse-spark-7.1', models)?.id).toBe('muse-spark-7.1-free');
  });

  test('unknown ids resolve to undefined', () => {
    expect(resolveCatalogModel('nope', models)).toBeUndefined();
    expect(resolveCatalogModel('mimo-v2.5-pro', models)).toBeUndefined();
    expect(resolveCatalogModel('', models)).toBeUndefined();
  });

  test('caller maps alias to upstream id via canonical_id', () => {
    const alias = resolveCatalogModel('muse-spark-7.1-free', models);
    expect(alias).toBeDefined();
    expect(alias!.canonical_id || alias!.id).toBe('muse-spark-7.1-contributor-free');
  });
});


test('discovery agrees with the installed OpenCode2 Zen catalog and captured endpoints', () => {
  const catalog = buildZenCatalog(nativeCapture.upstream, nativeCapture.metadata);
  const canonical = catalog.models.filter(m => !m.canonical_id).map(m => ({ id: m.id, endpoint: m.endpoint }));
  expect(canonical).toEqual(nativeCapture.native_models);
  for (const request of nativeCapture.requests) {
    const selected = resolveCatalogModel(request.model, catalog.models);
    expect(selected?.endpoint).toBe(request.path.endsWith('/responses') ? 'responses' : 'chat');
    expect(request.status).toBe(200);
  }
});
