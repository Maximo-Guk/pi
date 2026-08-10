import type { SessionSearch, SessionSearchHit } from "./index.ts";

export interface SearchIndexWriter<TItem = unknown> {
	apply(items: TItem[]): Promise<void>;
	flush?(): Promise<void>;
}

export interface IndexedSessionSearch<T extends SessionSearchHit = SessionSearchHit, TItem = unknown>
	extends SessionSearch<T>,
		SearchIndexWriter<TItem> {}
