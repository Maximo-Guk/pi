import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { NodeExecutionEnv } from "../../../src/harness/env/nodejs.ts";
import {
	InMemorySessionStorage,
	JsonlSessionRepo,
	Session,
	type SessionMetadata,
	type SessionStorage,
} from "../../../src/harness/session/index.ts";
import {
	createJsonlScanningSessionSearch,
	createMemoryScanningSessionSource,
	createScanningSessionSearch,
	type ScanningSessionSearchHit,
	type ScanningSessionSource,
	type SearchIndexWriter,
} from "../../../src/search/index.ts";
import type { AgentMessage } from "../../../src/types.ts";

interface WorkspaceMetadata extends SessionMetadata {
	cwd: string;
}

interface SearchDocument<TMetadata extends SessionMetadata> {
	sessionId: string;
	entryId: string;
	seq: number;
	timestamp: number;
	metadata: TMetadata;
	text: string;
	fields?: Record<string, unknown>;
}

type SearchDocumentFeedItem<TMetadata extends SessionMetadata> =
	| { type: "entry_upsert"; document: SearchDocument<TMetadata> }
	| { type: "entry_delete"; sessionId: string; entryId: string }
	| { type: "session_delete"; sessionId: string }
	| { type: "session_metadata"; sessionId: string; metadata: TMetadata };

const tempDirs: string[] = [];

function createTempDir(): string {
	const directory = mkdtempSync(join(tmpdir(), "pi-agent-search-"));
	tempDirs.push(directory);
	return directory;
}

afterEach(() => {
	while (tempDirs.length > 0) rmSync(tempDirs.pop()!, { recursive: true, force: true });
});

function message(text: string): AgentMessage {
	return { role: "user", content: [{ type: "text", text }], timestamp: 1 };
}

function createMemorySession(metadata: WorkspaceMetadata): Session<WorkspaceMetadata> {
	return new Session<WorkspaceMetadata>(
		new InMemorySessionStorage(metadata) as unknown as SessionStorage<WorkspaceMetadata>,
	);
}

function createSource(sessions: Session<WorkspaceMetadata>[]): ScanningSessionSource<WorkspaceMetadata> {
	return createMemoryScanningSessionSource(sessions);
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
	const items: T[] = [];
	for await (const item of iterable) items.push(item);
	return items;
}

async function feedDocumentSnapshot<TMetadata extends SessionMetadata, TListOptions>(
	source: ScanningSessionSource<TMetadata, TListOptions>,
	index: SearchIndexWriter<SearchDocumentFeedItem<TMetadata>>,
	listOptions?: TListOptions,
): Promise<void> {
	for await (const session of source.sessions(listOptions)) {
		const metadata = await session.metadata();
		await index.apply([{ type: "session_metadata", sessionId: metadata.id, metadata }]);
		for await (const candidate of session.entries()) {
			await index.apply([
				{
					type: "entry_upsert",
					document: {
						sessionId: metadata.id,
						entryId: candidate.entryId,
						seq: candidate.seq,
						timestamp: candidate.timestamp,
						metadata,
						text: candidate.text,
						fields: candidate.fields,
					},
				},
			]);
		}
	}
}

class InMemoryIndexedSearch<TMetadata extends SessionMetadata>
	implements SearchIndexWriter<SearchDocumentFeedItem<TMetadata>>
{
	readonly appliedBatches: SearchDocumentFeedItem<TMetadata>[][] = [];
	private readonly documents = new Map<string, SearchDocument<TMetadata>>();
	private readonly metadata = new Map<string, TMetadata>();

	async apply(items: SearchDocumentFeedItem<TMetadata>[]): Promise<void> {
		this.appliedBatches.push(items);
		for (const item of items) {
			switch (item.type) {
				case "entry_upsert":
					this.documents.set(`${item.document.sessionId}:${item.document.entryId}`, item.document);
					this.metadata.set(item.document.sessionId, item.document.metadata);
					break;
				case "entry_delete":
					this.documents.delete(`${item.sessionId}:${item.entryId}`);
					break;
				case "session_delete":
					for (const key of [...this.documents.keys()]) {
						if (key.startsWith(`${item.sessionId}:`)) this.documents.delete(key);
					}
					this.metadata.delete(item.sessionId);
					break;
				case "session_metadata":
					this.metadata.set(item.sessionId, item.metadata);
					break;
			}
		}
	}

	async *search(text: string, options: { limit?: number } = {}): AsyncIterable<ScanningSessionSearchHit> {
		const query = text.trim().toLowerCase();
		if (!query) return;
		let count = 0;
		for (const document of [...this.documents.values()].sort((left, right) => left.seq - right.seq)) {
			if (!document.text.toLowerCase().includes(query)) continue;
			yield {
				sessionId: document.sessionId,
				entryId: document.entryId,
				timestamp: document.timestamp,
				snippet: document.text,
			};
			count += 1;
			if (options.limit !== undefined && count >= options.limit) break;
		}
	}
}

interface EntryReferenceFeedItem {
	sessionId: string;
	entryId: string;
	seq: number;
}

class EntryReferenceIndex implements SearchIndexWriter<EntryReferenceFeedItem> {
	readonly items: EntryReferenceFeedItem[] = [];

	async apply(items: EntryReferenceFeedItem[]): Promise<void> {
		this.items.push(...items);
	}
}

describe("session search", () => {
	it("scans an arbitrary in-memory projected source", async () => {
		const root = createMemorySession({ id: "root", createdAt: 1, cwd: "/repo" });
		await root.appendMessage(message("fix auth flow"));
		const other = createMemorySession({ id: "other", createdAt: 2, cwd: "/other" });
		await other.appendMessage(message("auth in another workspace"));
		const search = createScanningSessionSearch(createSource([root, other]));

		expect("apply" in search).toBe(false);
		expect(await collect(search.search("auth"))).toMatchObject([{ sessionId: "root" }, { sessionId: "other" }]);
		expect(await collect(search.search("missing"))).toEqual([]);
	});

	it("includes labels in memory scanning projections", async () => {
		const session = createMemorySession({ id: "session", createdAt: 1, cwd: "/repo" });
		const entryId = await session.appendMessage(message("plain body"));
		await session.setLabel(entryId, "important label");
		const search = createScanningSessionSearch(createSource([session]));

		expect(await collect(search.search("important"))).toMatchObject([{ sessionId: "session", entryId }]);
	});

	it("honors entry type filters and abort signals in scanning search", async () => {
		const session = createMemorySession({ id: "session", createdAt: 1, cwd: "/repo" });
		const messageEntryId = await session.appendMessage(message("auth message"));
		await session.appendCustomEntry("note", { text: "auth custom" });
		const search = createScanningSessionSearch(createSource([session]));

		expect(await collect(search.search("auth", { entryTypes: ["message"] }))).toMatchObject([
			{ sessionId: "session", entryId: messageEntryId },
		]);

		const controller = new AbortController();
		controller.abort();
		await expect(collect(search.search("auth", { signal: controller.signal }))).rejects.toMatchObject({
			name: "AbortError",
		});
	});

	it("feeds memory projections into an arbitrary index without a repository search method", async () => {
		const session = createMemorySession({ id: "session", createdAt: 1, cwd: "/repo" });
		const first = await session.appendMessage(message("implement auth search"));
		await session.appendMessage(message("unrelated"));
		const index = new InMemoryIndexedSearch<WorkspaceMetadata>();

		await feedDocumentSnapshot(createSource([session]), index);

		expect(await collect(index.search("auth"))).toMatchObject([{ sessionId: "session", entryId: first }]);
		expect(index.appliedBatches.length).toBeGreaterThan(1);
	});

	it("feeds projected snapshots through arbitrary backend-owned item shapes", async () => {
		const session = createMemorySession({ id: "session", createdAt: 1, cwd: "/repo" });
		const entryId = await session.appendMessage(message("index by reference"));
		const index = new EntryReferenceIndex();

		for await (const scanningSession of createSource([session]).sessions()) {
			const metadata = await scanningSession.metadata();
			for await (const candidate of scanningSession.entries()) {
				await index.apply([{ sessionId: metadata.id, entryId: candidate.entryId, seq: candidate.seq }]);
			}
		}

		expect(index.items).toEqual([{ sessionId: "session", entryId, seq: 1 }]);
	});

	it("scans JSONL sessions from disk through the JSONL scanning source", async () => {
		const root = createTempDir();
		const options = { fs: new NodeExecutionEnv({ cwd: root }), sessionsRoot: root };
		const repository = new JsonlSessionRepo(options);
		const cwd = join(root, "workspace");
		const otherCwd = join(root, "other");
		const session = await repository.create({ id: "jsonl", cwd });
		const entryId = await session.appendMessage(message("jsonl backed auth entry"));
		await session.setLabel(entryId, "disk label");
		const other = await repository.create({ id: "other", cwd: otherCwd });
		const otherEntryId = await other.appendMessage(message("jsonl backed auth entry in another cwd"));
		const search = createJsonlScanningSessionSearch(options);

		const authHits = await collect(search.search("auth"));
		expect(authHits).toHaveLength(2);
		expect(authHits).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					sessionId: "jsonl",
					metadata: expect.objectContaining({ id: "jsonl", cwd }),
					entryId,
				}),
				expect.objectContaining({
					sessionId: "other",
					metadata: expect.objectContaining({ id: "other", cwd: otherCwd }),
					entryId: otherEntryId,
				}),
			]),
		);
		expect(await collect(search.search("disk"))).toMatchObject([
			{ sessionId: "jsonl", metadata: { id: "jsonl", cwd }, entryId },
		]);
	});

	it("keeps index failures outside canonical session writes", async () => {
		const session = createMemorySession({ id: "session", createdAt: 1, cwd: "/repo" });
		await session.appendMessage(message("before index failure"));
		const source = createSource([session]);
		const failingIndex = {
			async apply(_items: SearchDocumentFeedItem<WorkspaceMetadata>[]) {
				throw new Error("index down");
			},
		};

		await expect(feedDocumentSnapshot(source, failingIndex)).rejects.toThrow("index down");
		await session.appendMessage(message("after index failure"));

		await expect(session.findEntries({ type: "message" })).resolves.toHaveLength(2);
	});
});
