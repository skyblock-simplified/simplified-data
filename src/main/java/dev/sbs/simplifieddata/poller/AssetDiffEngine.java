package dev.sbs.simplifieddata.poller;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.source.ManifestIndex;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Pure-function delta engine that compares a manifest snapshot against the persisted
 * per-entry state rows and emits an {@link AssetDiff} describing additions, content changes,
 * and removals.
 *
 * <p>Intentionally stateless. No database access, no HTTP calls, no logging. Every input is
 * supplied by the caller; every output is returned in the result. Fully unit-testable in
 * isolation.
 *
 * <p>The engine treats {@link ManifestIndex.Entry#getPath()} as the join key on the manifest
 * side and {@link ExternalAssetEntryState#getEntryPath()} as the join key on the state side.
 * {@code _extra} companion files are NOT tracked as separate state rows in Phase 4c - only
 * the primary file's {@code content_sha256} drives the diff. See the plan's risk section R7
 * for the rationale and the Phase 5 follow-up.
 *
 * @see AssetDiff
 * @see AssetPoller
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AssetDiffEngine {

    /**
     * Computes the delta between a fresh manifest and the current per-entry persisted state.
     *
     * <p>Classification rules:
     * <ul>
     *   <li><b>added</b> - manifest entry whose path does not exist in the persisted state.</li>
     *   <li><b>changed</b> - manifest entry whose path exists in the persisted state AND
     *       whose {@code content_sha256} differs from the persisted {@code entrySha256}.</li>
     *   <li><b>removed</b> - persisted state row whose path is not in the manifest.</li>
     * </ul>
     *
     * <p>No-op entries (path present on both sides, hashes match) are silently dropped - they
     * do not appear in any of the three output lists.
     *
     * @param manifest the fresh manifest fetched from GitHub
     * @param persisted the current per-entry state rows loaded from {@code external_asset_entry_state}
     * @return the computed diff
     */
    public static @NotNull AssetDiff compute(
        @NotNull ManifestIndex manifest,
        @NotNull ConcurrentList<ExternalAssetEntryState> persisted
    ) {
        ConcurrentMap<String, ExternalAssetEntryState> persistedByPath = Concurrent.newMap();
        for (ExternalAssetEntryState row : persisted)
            persistedByPath.put(row.getEntryPath(), row);

        ConcurrentList<ManifestIndex.Entry> added = Concurrent.newList();
        ConcurrentList<AssetDiff.ChangedEntry> changed = Concurrent.newList();

        for (ManifestIndex.Entry entry : manifest.getFiles()) {
            ExternalAssetEntryState existing = persistedByPath.remove(entry.getPath());

            if (existing == null) {
                added.add(entry);
                continue;
            }

            if (!existing.getEntrySha256().equals(entry.getContentSha256()))
                changed.add(new AssetDiff.ChangedEntry(entry, existing));
        }

        ConcurrentList<ExternalAssetEntryState> removed = Concurrent.newList();
        persistedByPath.values().forEach(removed::add);

        if (added.isEmpty() && changed.isEmpty() && removed.isEmpty())
            return AssetDiff.empty();

        return AssetDiff.of(added, changed, removed);
    }

}
