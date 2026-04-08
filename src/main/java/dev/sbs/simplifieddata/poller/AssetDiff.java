package dev.sbs.simplifieddata.poller;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.source.ManifestIndex;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable result of an {@link AssetDiffEngine#compute} call, describing the three
 * categories of per-entry state change that the Phase 4c poller emits into the
 * {@code external_asset_entry_state} table.
 *
 * <p>Instances are constructed exclusively via
 * {@link #of(ConcurrentList, ConcurrentList, ConcurrentList)} or the {@link #empty()}
 * sentinel - the constructor is package-private to the static factories so the lists are
 * always unmodifiable views.
 *
 * @see AssetDiffEngine
 * @see AssetPoller
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AssetDiff {

    /** Manifest entries whose path is not yet in the persisted state. Always unmodifiable. */
    private final @NotNull ConcurrentList<ManifestIndex.Entry> added;

    /** Manifest entries whose {@code content_sha256} differs from the persisted {@code entrySha256}. Always unmodifiable. */
    private final @NotNull ConcurrentList<ChangedEntry> changed;

    /** Persisted entry rows whose path is no longer in the manifest. Always unmodifiable. */
    private final @NotNull ConcurrentList<ExternalAssetEntryState> removed;

    /**
     * Constructs a diff from the three delta lists.
     *
     * <p>All three input lists are copied into unmodifiable views so callers cannot mutate
     * the returned instance after construction.
     *
     * @param added manifest entries new to the persisted state
     * @param changed manifest entries whose content hash changed
     * @param removed persisted entries no longer in the manifest
     * @return a new immutable diff
     */
    public static @NotNull AssetDiff of(
        @NotNull ConcurrentList<ManifestIndex.Entry> added,
        @NotNull ConcurrentList<ChangedEntry> changed,
        @NotNull ConcurrentList<ExternalAssetEntryState> removed
    ) {
        return new AssetDiff(
            Concurrent.newUnmodifiableList(added),
            Concurrent.newUnmodifiableList(changed),
            Concurrent.newUnmodifiableList(removed)
        );
    }

    /**
     * Returns a sentinel empty diff, used when the engine observes no changes.
     *
     * @return an immutable diff with three empty lists
     */
    public static @NotNull AssetDiff empty() {
        return new AssetDiff(
            Concurrent.newUnmodifiableList(),
            Concurrent.newUnmodifiableList(),
            Concurrent.newUnmodifiableList()
        );
    }

    /**
     * Returns whether this diff carries any changes.
     *
     * @return {@code true} when all three lists are empty
     */
    public boolean isEmpty() {
        return this.added.isEmpty() && this.changed.isEmpty() && this.removed.isEmpty();
    }

    /**
     * Pairing of a fresh manifest entry with the persisted state row it is replacing.
     *
     * <p>Both sides are carried so log messages can surface the old and new content hashes
     * for operator observability, and so the {@link AssetPoller} can re-key the state row
     * correctly if the manifest ever evolves to carry a denormalised key alongside the path.
     */
    @Getter
    @RequiredArgsConstructor
    public static final class ChangedEntry {

        /** The fresh manifest entry carrying the new {@code content_sha256}. */
        private final @NotNull ManifestIndex.Entry current;

        /** The persisted state row being replaced, carrying the old hash and timestamp. */
        private final @NotNull ExternalAssetEntryState previous;

    }

}
