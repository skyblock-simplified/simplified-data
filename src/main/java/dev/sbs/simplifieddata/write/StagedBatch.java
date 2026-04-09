package dev.sbs.simplifieddata.write;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Per-source staging output produced by
 * {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource#stageBatch(ConcurrentMap)}
 * as the first phase of the Phase 6b.1 two-phase commit path.
 *
 * <p>A {@code StagedBatch} carries:
 * <ul>
 *   <li>the target entity class (for the per-type count in commit messages and
 *       retry escalation),</li>
 *   <li>the file snapshots map - one entry per dirty file on this source, keyed
 *       by repo-root-relative path, valued by the full mutated entity list
 *       that should be written to that file,</li>
 *   <li>the list of mutations that drove this staging (for dead-letter
 *       escalation via
 *       {@link WriteQueueConsumer#scheduleRetry(DelayedWriteRequest)} when the
 *       cross-source commit fails),</li>
 *   <li>and the mutation count for summary reporting.</li>
 * </ul>
 *
 * <p>Staging is a pure local transformation - no network I/O happens when the
 * caller reads these fields. The {@link dev.sbs.simplifieddata.write.WriteBatchScheduler}
 * merges {@code StagedBatch} instances across every dirty source in the current
 * tick into a single {@link BatchCommitRequest} before handing it to
 * {@link GitDataCommitService} for the actual GitHub commit.
 *
 * <p>A {@code StagedBatch} may be {@link #isEmpty() empty} - this means the
 * source drained at least one mutation from its buffer but every mutation was
 * a no-op (e.g. an upsert of an entity that was already byte-identical to the
 * current on-disk state, or a delete of an id that was not present in any
 * file). Empty batches are silently ignored by the scheduler.
 *
 * @param <T> the entity type
 * @see WritableRemoteJsonSource#stageBatch(ConcurrentMap)
 * @see BatchCommitRequest
 */
@Getter
public final class StagedBatch<T extends JpaModel> {

    private static final @NotNull StagedBatch<?> EMPTY = new StagedBatch<>(
        JpaModel.class,
        Concurrent.newUnmodifiableMap(),
        Concurrent.newUnmodifiableList(),
        0
    );

    /** The entity class this staging targets. */
    private final @NotNull Class<? extends JpaModel> modelClass;

    /**
     * File snapshots keyed by repo-root-relative path. Each value is the full
     * mutated entity list that should replace the current on-disk content of
     * that file. Only dirty files are present - files whose serialized content
     * would be byte-identical after mutation dispatch are omitted so the Git
     * Data commit does not create a no-op blob.
     */
    private final @NotNull ConcurrentMap<String, ConcurrentList<T>> fileSnapshots;

    /**
     * The buffered mutations that drove this staging. Retained for
     * dead-letter escalation: on a commit failure, every mutation in this
     * list is re-queued via
     * {@link WriteQueueConsumer#scheduleRetry(DelayedWriteRequest)}.
     */
    private final @NotNull ConcurrentList<BufferedMutation<T>> mutations;

    /** The mutation count applied during staging, used for summary logs. */
    private final int mutationCount;

    /**
     * Public constructor. Typically constructed via
     * {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource#stageBatch()}
     * (cross-package call) and the {@link #empty()} sentinel. The visibility is
     * public because {@code WritableRemoteJsonSource} lives in the sibling
     * {@code dev.sbs.simplifieddata.persistence} package.
     */
    public StagedBatch(
        @NotNull Class<? extends JpaModel> modelClass,
        @NotNull ConcurrentMap<String, ConcurrentList<T>> fileSnapshots,
        @NotNull ConcurrentList<BufferedMutation<T>> mutations,
        int mutationCount
    ) {
        this.modelClass = modelClass;
        this.fileSnapshots = fileSnapshots;
        this.mutations = mutations;
        this.mutationCount = mutationCount;
    }

    /** @return {@code true} when no dirty files are staged. */
    public boolean isEmpty() {
        return this.fileSnapshots.isEmpty();
    }

    /** @return the typed sentinel for an empty staging result. */
    @SuppressWarnings("unchecked")
    public static <T extends JpaModel> @NotNull StagedBatch<T> empty() {
        return (StagedBatch<T>) EMPTY;
    }

    /**
     * Returns the buffered mutations as an untyped collection for aggregation
     * across heterogeneous {@code StagedBatch} instances in
     * {@link BatchCommitRequest#builder()}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public @NotNull Collection<BufferedMutation<?>> getMutationsUntyped() {
        return (Collection) this.mutations;
    }

}
