# Session Search API design

Introduce a minimal, generic search interface for Pi session entries. The interface is intentionally small: it exposes only the query text, optional limit, cancellation signal, and an entry‑type filter.

## Goals
- **Low latency & search‑as‑you‑type** – the first matches must be available within a few milliseconds so the UI can show results while the user continues typing.
- **Sessions may contain thousands (or more) of entries** – the implementation must avoid allocating large intermediate arrays.
- **Cancel‑on‑change** – when the user refines the query, any in‑flight search must be abortable without wasting work.
- **Backend agnosticism** – the same interface must work for scanning (JSONL, memory), indexed (SQLite FTS), or remote (Elasticsearch, etc.) implementations.
- **No writer lease taken** – a search source must only read sessions; it must not call `SessionRepo.open()` on a Harness‑owned session.

These requirements lead us to an **async iterable** (`AsyncIterable<SessionSearchHit>`) as the return type, which delivers results incrementally, supports cancellation via `AbortSignal`, and keeps memory usage low.

## Non-Goals
- **Search indexing** – it is entirely up to the search backend implementation to handle potential indexing. All other concerns—how sessions are enumerated, how entries are read, whether the backend scans or indexes, how it builds or updates its index, pagination, ranking, caching, freshness, batching, replay, or repair—are strictly implementation details.


## Core Types

### Hit Identity

```ts
export interface SessionSearchHit {
  /** Logical identifier of the session that owns the entry. */
  readonly sessionId: string;

  /** Logical identifier of the entry within that session. */
  readonly entryId: string;
}
```

- `(sessionId, entryId)` is the stable, backend‑independent identity of a search result.
- No timestamp, score, or snippet is included by default. Backends may extend the hit type with additional fields (e.g. `score`, `snippet`) if they wish to expose backend‑specific metadata. Consumers that need such data must load the entry from the canonical session state using the returned identifiers.

### Query Options

```ts
export interface SessionSearchOptions {
  /** Restrict results to specific entry types (e.g. ["message","compaction"]). */
  readonly entryTypes?: readonly Entry["type"][];

  /** Maximum number of hits to return. */
  readonly limit?: number;

  /** Abort signal for cancellation (e.g. for search‑as‑you‑type). */
  readonly signal?: AbortSignal;
}
```

- `entryTypes` is a generic filter based on the canonical `Entry["type"]` field. It allows callers to limit results to message entries, compaction entries, etc., without leaking storage‑specific concepts.
- `limit` is advisory; a backend may return fewer results if it cannot satisfy the request.
- `signal` enables safe cancellation of in‑flight searches (e.g. when the user changes the search term).

### Search Interface

```ts
export interface SessionSearch<T extends SessionSearchHit = SessionSearchHit> {
  /**
   * Search over committed session entries.
   *
   * @param text    – query string (trimmed, case‑insensitive by default)
   * @param options – optional paging, cancellation, and entry‑type filter
   * @returns an async iterable of hits in the order the backend chooses
   */
  search(
    text: string,
    options?: SessionSearchOptions,
  ): AsyncIterable<T>;
}
```

- The method returns an `AsyncIterable` (i.e. an object with a `[Symbol.asyncIterator]` method). This enables `for await (const hit of search.search(...))` loops.
- The async iterable hides pagination, buffering, and any backend‑specific cursor state. Consumers simply iterate until the iterator is exhausted or they break out.
- The order of hits is not guaranteed to be deterministic across backends; it is whatever the backend deems most relevant (e.g. BM25 score for FTS, or source order for a linear scanner). If a backend wishes to expose a score, it should do so by extending the hit type.

## Usage Examples

### Basic Scan

```ts
import { createScanningSessionSearch } from "@earendil-works/pi-agent-core";
import type { Session } from "@earendil-works/pi-agent-core/harness/session/types.ts";

// Imagine we have a Map of already‑owned Session objects (e.g. from tests)
const sessionsMap = new Map<string, Session>([/* ... */]);
// Simple async iterable over the session values
const source = {
  async *[Symbol.asyncIterator]() {
    for (const session of sessionsMap.values()) {
      yield session;
    }
  },
};
const search = createScanningSessionSearch(source);

for await (const hit of search.search("authentication", { limit: 10 })) {
  const session = sessionsMap.get(hit.sessionId)!;
  const entry = await session.getEntry(hit.entryId);
  console.log(`Found in ${session.id}: ${entry.id}`);
}
```

### SQLite FTS with Entry‑Type Filter

```ts
import { createSqliteSessionSearch } from "@earendil-works/pi-session-backend-sqlite-node";
import { createNodeSqliteFactory } from "@earendil-works/pi-session-backend-sqlite-node";

const opts = {
  env: { /* filesystem abstraction */ },
  sqlite: createNodeSqliteFactory(),
  databasePath: "./sessions.sqlite",
};
const search = createSqliteSessionSearch(opts);

// Search only message and compaction entries, limit 20 results
for await (const hit of search.search("auth", {
  entryTypes: ["message", "compaction"],
  limit: 20,
})) {
  // Assuming a session store is available; e.g., a Map<string, Session> or a storage lookup.
  const session = await sessionStore.get(hit.sessionId);
  const entry = await session.getEntry(hit.entryId);
  // Use session and entry as needed (e.g. display snippet)
}
```

### Search‑as‑you‑type with Cancellation

```ts
let currentAbortController: AbortController | null = null;

async function updateResults(query: string) {
  // Cancel any previous search
  currentAbortController?.abort();

  const controller = new AbortController();
  currentAbortController = controller;

  try {
    for await (const hit of search.search(query, {
      limit: 10,
      signal: controller.signal,
    })) {
      // Render hit …
    }
  } catch (err) {
    if (err.name !== "AbortError") throw err;
    // Expected when the search was cancelled
  }
}
```

---

Note: The API is agnostic to the number of sessions. A search source may expose a single session (single‑session setup) or many sessions (a repo). The same interface works in both cases.

### Example: Custom search backend updating index via harness events
```ts
import { AgentHarness, Session } from "@earendil-works/pi-agent-core";
import type { Entry, SessionMetadata, AgentMessage } from "@earendil-works/pi-agent-core/types.ts";

// Assume we have a search backend that implements the minimal API and also
// provides an internal method to add/remove entries from its index.
class MySearchBackend implements SessionSearch {
  // ... implement search() returning AsyncIterable<SessionSearchHit> ...

  // Internal index update methods (implementation‑specific)
  private addToIndex(metadata: SessionMetadata, entry: Entry): void { /* … */ }
  private removeFromIndex(entryId: string): void { /* … */ }

  constructor(private harness: AgentHarness) {
    // Listen to harness events that signal a committed change.
    this.harness.events.on("message_end", async ({ entryId }) => {
      // The event only tells us that something changed; we fetch the
      // latest committed state and update our index.
      await this.refreshEntry(entryId);
    });
    this.harness.events.on("entry_added", async ({ entryId }) => {
      await this.refreshEntry(entryId);
    });
    this.harness.events.on("fact_update", async ({ kind, targetId }) => {
      if (kind === "name" || kind === "label") {
        // A name/label change may affect the searchable text of an entry.
        await this.refreshEntry(targetId);
      }
    });
  }

  private async refreshEntry(entryId: string) {
    // Find the session that owns this entry (we could keep a map or
    // query the harness for sessions; here we scan all sessions for simplicity).
    for await (const session of this.harness.lanes().then(ls =>
      Promise.all(ls.map(l => this.harness.lane(l.name))))
    ) {
      try {
        const entry = await session.getEntry(entryId);
        if (entry) {
          const meta = await session.getMetadata();
          // Update index – this is backend‑specific.
          this.addToIndex(meta, entry);
          return; // entry found, stop searching
        }
      } catch {
        // Entry not in this session; continue.
      }
    }
    // If we get here the entry was deleted.
    this.removeFromIndex(entryId);
  }
}
```
## What Is Generic

The following belong to the shared contract and must be implemented by every `SessionSearch`:

- The query `text` string.
- Optional `limit` on the number of results.
- Optional `signal` for cancellation.
- Optional `entryTypes` filter based on the canonical `Entry["type"]` field.
- The hit type providing `sessionId` and `entryId` as stable logical identifiers.
- Delivery of results as an `AsyncIterable<SessionSearchHit>` (i.e. a cancellable, ordered stream).

Everything else is strictly an implementation detail:

- How sessions are discovered (JSONL files, memory objects, SQLite tables, remote API, etc.).
- Whether the backend scans on every query or maintains an index.
- How the backend projects entry content into searchable text (e.g. stripping labels, extracting only the user‑visible portion of a message, etc.).
- Whether the backend uses tokenization, stemming, BM25, embeddings, or any other ranking algorithm.
- How the backend handles pagination, buffering, and internal cursors.
- How the bootstrap, rebuild, incremental update, deletion, and repair of the index are performed.
- How the backend integrates with harness events (e.g. using `message_end`, `entry_added`, `fact_update` as wake‑ups for application‑owned catch‑up jobs).
- Freshness guarantees (eventual, scan‑on‑demand, etc.).
- Any backend‑specific authentication, configuration, or resource management.

---

### Fail‑fast behavior

- **Error propagation** – If reading a session’s metadata, entry list, or an individual entry throws, the error must be propagated to the consumer of the `AsyncIterable`. The iterator’s `next()` promise should reject with the error; the search operation stops immediately.

- **Duplicate session IDs** – The backend must track seen `sessionId` values while iterating over the source. Encountering a duplicate `sessionId` must cause an immediate error (e.g., `new Error('Duplicate sessionId: ${sessionId})`) that is propagated as above.

- **Snapshot consistency for indexing backends** – An indexing backend must obtain a read‑only, transactionally consistent snapshot of the session stream before indexing (e.g., using the harness’s committed‑only API). If a consistent snapshot cannot be obtained, the backend must abort and propagate an error indicating the inconsistency.

## Relation to Harness V2 and Pi Philosophy
- **Search is external derived state.**  
  The harness remains the sole writer of canonical session state (entries, lane records, lanes, facts, stats). Search indexes are disposable and may be rebuilt at any time without affecting harness recovery.

- **No writer lease is taken.**  
  A scanning source must only open sessions for reading (e.g. via `loadJsonlSessionStorage` read‑only, or by wrapping already‑owned `Session` objects). The SQLite FTS implementation never calls `SessionRepo.open()` for its searches; it only reads from the database file.

- **Events are hints, not a durable feed.**  
  Applications may use harness events (`message_end`, `entry_added`, `fact_update`) to schedule application‑owned catch‑up or rebuild jobs, but durable indexing must not depend on event delivery order or guarantees. The checkpoint for an index is `(sessionId, lastEntrySeq)` advanced only after the backend has successfully applied indexed entries.

- **Small interface, rich implementations.**  
  The core agent package provides only the `SessionSearch` interface and a few convenient helpers (JSONL and memory scanning sources, SQLite FTS implementation). Applications are free to implement their own backends (Elasticsearch, Postgres FTS, etc.) without touching Pi internals.

- **Append‑only entries simplify indexing.**  
  Because session entries never change or are deleted once committed, a backend can safely maintain a per‑session cursor (`lastEntrySeq`) and only append newly committed entries to its index. Replays are naturally idempotent: if a backend crashes after indexing an entry but before advancing its cursor, a replay will see the entry already present and skip it.

---

## Comparison with the Existing `search-v1.md`

The current `packages/agent/docs/search-v1.md` describes a richer search design that includes:

- A `SessionSearchSource` with `list()` and `open()` (the latter may claim a writer lease).
- Indexed and scanning adapters that expose feed item types (`index_entry`, `rebuild`, etc.).
- A snapshot‑feeding utility (`feedSessionSnapshot`).
- A document‑projector convenience layer.
- Detailed consistency and crash‑behavior reasoning tied to the harness event model.

The minimal API proposed here keeps the spirit of those ideas (search as an external derived service) but reduces the shared contract to the absolute minimum needed for interoperability. Concrete backends may still expose richer administrative APIs (e.g. `apply()`, `flush()`, custom feed items) but those are not part of the universal `SessionSearch` type.

Implementations of the minimal API can be found in:

- `packages/agent/src/search/scanning.ts` – generic `ScanningSessionSearch`.
- `packages/agent/src/search/jsonl.ts` – JSONL scanning source and helper.
- `packages/agent/src/search/memory.ts` – memory scanning source and helper.
- `packages/session-backends/sqlite-node/src/sqlite/search-backend.ts` – SQLite FTS implementation (which already satisfies `SessionSearch` via `IndexedSessionSearch`).

No changes to `AgentHarness`, `Session`, `SessionStorage`, or `Effects` are required. Search remains an external service that consumes only committed session state.
## Default Implementations

The shared `SessionSearchHit` stays minimal. Built-in implementations may return extended hit types when they can provide useful display data without making that data part of the universal contract.

### Scanning search hits

```ts
export interface ScanningSessionSearchHit extends SessionSearchHit {
  /** Entry commit time, available while scanning the candidate. */
  readonly timestamp: number;

  /** Searchable text that matched, suitable as a basic snippet. */
  readonly snippet: string;
}

export function createScanningSessionSearch(
  source: ScanningSessionSource,
): SessionSearch<ScanningSessionSearchHit>;
```

A scanner already reads each matching candidate, so returning `timestamp` and `snippet` avoids throwing away data the implementation already has. It does not define ranking or require a `score`.

### JSONL scanning hits

```ts
export interface JsonlSessionSearchHit extends ScanningSessionSearchHit {
  /** JSONL session metadata read while scanning the session file. */
  readonly metadata: JsonlSessionMetadata;
}

export function createJsonlScanningSessionSearch(
  options: JsonlSessionRepoOptions,
): SessionSearch<JsonlSessionSearchHit>;
```

The JSONL adapter reads session metadata before scanning entries, so it can return that metadata as a JSONL-specific extension. Consumers typed against the base `SessionSearch` should rely only on `sessionId` and `entryId`; consumers typed against the JSONL implementation may use `metadata`, `timestamp`, and `snippet` to render results without immediately re-reading the session file.
