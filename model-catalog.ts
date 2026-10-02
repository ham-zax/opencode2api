// model-catalog.ts — pure Zen-only free chat-model discovery.
//
// No imports, no network, no globals, no startup, no side effects.
// The gateway parent calls buildZenCatalog(upstream, metadata) with:
//   upstream: the parsed `GET /v1/models` body ({ data: [...] })
//   metadata: the parsed models.dev api.json document (uses ONLY the
//             `opencode` provider namespace; opencode-go and every other
//             provider namespace are ignored)
// and retains its last successful catalog when this module throws.

export type ZenModelEndpoint = 'chat' | 'responses';

export interface ZenModel {
  id: string;
  object: string;
  created?: number;
  owned_by?: string;
  endpoint: ZenModelEndpoint;
  canonical_id?: string;
  [key: string]: unknown;
}

export interface ZenCatalog {
  models: ZenModel[];
  excluded: { id: string; reason: string }[];
}

// Jev models answer on /v1/systemone with structured questions, not text
// generation (per the official Zen docs), so they are never chat models.
const JEV_RE = /^jev(?:-|$)/i;

// Compatibility aliases are derived, never hardcoded per version.
const CONTRIBUTOR_RE = /^muse-spark-(.+)-contributor-free$/;
const MUSE_FREE_RE = /^muse-spark-(.+)-free$/;
const MUSE_RE = /^muse-spark-.+/;

const NPM_CHAT = '@ai-sdk/openai-compatible';
const NPM_RESPONSES = '@ai-sdk/openai';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function buildZenCatalog(upstream: unknown, metadata: unknown): ZenCatalog {
  if (!isRecord(upstream) || !Array.isArray(upstream.data)) {
    throw new Error('buildZenCatalog: upstream must be an object with a data array');
  }
  const providerDoc = isRecord(metadata) ? metadata['opencode'] : undefined;
  if (!isRecord(providerDoc)) {
    throw new Error('buildZenCatalog: metadata must contain a nonempty opencode.models object');
  }
  const registry = providerDoc['models'];
  if (!isRecord(registry) || Object.keys(registry).length === 0) {
    throw new Error('buildZenCatalog: metadata must contain a nonempty opencode.models object');
  }
  const providerNpm =
    typeof providerDoc['npm'] === 'string' && providerDoc['npm'] ? (providerDoc['npm'] as string) : undefined;

  const models: ZenModel[] = [];
  const excluded: { id: string; reason: string }[] = [];
  const seen = new Set<string>();

  for (const raw of upstream.data as unknown[]) {
    const entry = isRecord(raw) ? raw : undefined;
    const id = entry?.['id'];
    if (typeof id !== 'string' || !id) {
      excluded.push({ id: '(missing id)', reason: 'upstream entry has no id' });
      continue;
    }
    if (seen.has(id)) continue;
    seen.add(id);

    const meta = isRecord(registry[id]) ? (registry[id] as Record<string, unknown>) : undefined;
    if (!meta) {
      excluded.push({ id, reason: 'missing metadata: no opencode.models entry, no speculative routing' });
      continue;
    }

    // Free means exactly numeric zero. ID suffixes never imply pricing.
    const cost = isRecord(meta['cost']) ? (meta['cost'] as Record<string, unknown>) : undefined;
    const cacheReadPresent = cost !== undefined && 'cache_read' in cost;
    const cacheWritePresent = cost !== undefined && 'cache_write' in cost;
    if (
      cost === undefined ||
      cost['input'] !== 0 ||
      cost['output'] !== 0 ||
      (cacheReadPresent && cost['cache_read'] !== 0) ||
      (cacheWritePresent && cost['cache_write'] !== 0)
    ) {
      excluded.push({ id, reason: 'not free: cost.input/output and any cache_read/cache_write must be exactly 0' });
      continue;
    }

    // Retired entries are still advertised by upstream.
    if (meta['status'] === 'deprecated') {
      excluded.push({ id, reason: 'deprecated upstream, retired' });
      continue;
    }

    if (JEV_RE.test(id)) {
      excluded.push({ id, reason: 'non-chat model: jev family serves /v1/systemone, not text generation' });
      continue;
    }

    const modalities = isRecord(meta['modalities']) ? (meta['modalities'] as Record<string, unknown>) : undefined;
    const output = modalities?.['output'];
    if (!Array.isArray(output) || !(output as unknown[]).includes('text')) {
      excluded.push({ id, reason: 'unsupported output modalities: text output required' });
      continue;
    }

    // The gateway only translates Chat and Responses transports.
    const entryProvider = isRecord(meta['provider']) ? (meta['provider'] as Record<string, unknown>) : undefined;
    const npm =
      typeof entryProvider?.['npm'] === 'string' && entryProvider['npm']
        ? (entryProvider['npm'] as string)
        : (providerNpm ?? NPM_CHAT);
    let endpoint: ZenModelEndpoint | undefined;
    if (npm === NPM_RESPONSES) endpoint = 'responses';
    else if (npm === NPM_CHAT) endpoint = 'chat';
    if (endpoint === undefined) {
      excluded.push({ id, reason: `unsupported transport: ${npm}` });
      continue;
    }

    models.push({
      ...entry,
      id,
      object: typeof entry['object'] === 'string' ? (entry['object'] as string) : 'model',
      endpoint,
    });
  }

  // Compatibility aliases: muse-spark-(VERSION)-contributor-free is also
  // reachable as muse-spark-(VERSION)-free, unless that ID is already taken
  // by an actual selected model.
  const selectedIds = new Set(models.map((m) => m.id));
  for (const model of models) {
    const match = CONTRIBUTOR_RE.exec(model.id);
    if (!match) continue;
    const aliasId = `muse-spark-${match[1]}-free`;
    if (selectedIds.has(aliasId)) continue;
    selectedIds.add(aliasId);
    models.push({ ...model, id: aliasId, canonical_id: model.id });
  }

  return { models, excluded };
}

export function resolveCatalogModel(id: string, models: ZenModel[]): ZenModel | undefined {
  if (typeof id !== 'string' || !Array.isArray(models)) return undefined;
  const byId = new Map<string, ZenModel>();
  for (const model of models) {
    if (model && typeof model.id === 'string' && !byId.has(model.id)) byId.set(model.id, model);
  }
  const exact = byId.get(id);
  if (exact) return exact;

  // Generated muse-spark alias counterparts, in either direction.
  const contributor = CONTRIBUTOR_RE.exec(id);
  if (contributor) return byId.get(`muse-spark-${contributor[1]}-free`);
  const museFree = MUSE_FREE_RE.exec(id);
  if (museFree) return byId.get(`muse-spark-${museFree[1]}-contributor-free`);
  // Stripped IDs gain a -free suffix: muse-spark-X -> muse-spark-X-free,
  // and generically <id> -> <id>-free.
  if (MUSE_RE.test(id)) {
    return byId.get(`${id}-free`) ?? byId.get(`${id}-contributor-free`);
  }
  if (!id.endsWith('-free')) return byId.get(`${id}-free`);
  return undefined;
}
