package dev.sbs.simplifieddata.write;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * Cross-source aggregated batch passed from {@link WriteBatchScheduler}'s
 * staging phase to {@link GitDataCommitService} as the input to a single
 * multi-file GitHub commit.
 *
 * <p>A {@code BatchCommitRequest} is built by merging every non-empty
 * {@link StagedBatch} produced in a scheduler tick:
 * <ul>
 *   <li>{@link #fileContents} - repo-root-relative file path &rarr; the new
 *       serialized file body as a UTF-8 string. One entry per dirty file
 *       across every source. Distinct sources cannot produce conflicting
 *       entries because each source maps to exactly one entity type, and
 *       each entity type maps to a disjoint set of files.</li>
 *   <li>{@link #sourceBatches} - the staging inputs that produced the
 *       aggregated map, preserved for dead-letter escalation: on a commit
 *       failure, the scheduler iterates every mutation in every source batch
 *       and re-queues it via
 *       {@link WriteQueueConsumer#scheduleRetry(DelayedWriteRequest)}.</li>
 *   <li>{@link #totalMutationCount} - summary counter for the commit message
 *       and the INFO log line after a successful commit.</li>
 * </ul>
 *
 * <p>Construct via {@link #builder()} which accumulates sources incrementally
 * and guards against duplicate file paths across sources (which would indicate
 * a manifest schema bug - two entity classes mapped to the same file).
 *
 * @see StagedBatch
 * @see GitDataCommitService
 */
@Getter
public final class BatchCommitRequest {

    /** Repo-root-relative file path &rarr; new UTF-8 file body. */
    private final @NotNull ConcurrentMap<String, String> fileContents;

    /** Per-source staging inputs retained for dead-letter re-queueing. */
    private final @NotNull ConcurrentList<StagedBatch<?>> sourceBatches;

    /** Sum of every source's {@code mutationCount}. */
    private final int totalMutationCount;

    /**
     * Package-private constructor. Constructed via {@link #builder()}.
     */
    BatchCommitRequest(
        @NotNull ConcurrentMap<String, String> fileContents,
        @NotNull ConcurrentList<StagedBatch<?>> sourceBatches,
        int totalMutationCount
    ) {
        this.fileContents = fileContents;
        this.sourceBatches = sourceBatches;
        this.totalMutationCount = totalMutationCount;
    }

    /** @return {@code true} when the request has no dirty files. */
    public boolean isEmpty() {
        return this.fileContents.isEmpty();
    }

    /** @return the number of distinct file paths the commit will touch. */
    public int getDirtyFileCount() {
        return this.fileContents.size();
    }

    /** @return a new builder for accumulating per-source staging results. */
    public static @NotNull Builder builder() {
        return new Builder();
    }

    /**
     * Incremental accumulator used by {@link WriteBatchScheduler} to merge
     * every source's {@link StagedBatch} into a single cross-source
     * {@link BatchCommitRequest}.
     *
     * <p>Not thread-safe. The scheduler invokes it from its single scheduled
     * thread and the builder is discarded after {@link #build()}.
     */
    public static final class Builder {

        private final @NotNull ConcurrentMap<String, String> fileContents = Concurrent.newMap();
        private final @NotNull ConcurrentList<StagedBatch<?>> sourceBatches = Concurrent.newList();
        private int totalMutationCount = 0;

        /**
         * Adds a per-source staging result to the aggregate.
         *
         * <p>The supplied {@code serializer} is invoked once per dirty file
         * on the staged batch with the {@link StagedBatch#getFileSnapshots()
         * snapshot} list; it must return the UTF-8 JSON text to be written to
         * that file. The scheduler passes in a Gson-backed serializer so the
         * file body matches the entity type's existing JSON shape.
         *
         * @param staged the per-source staging result; empty batches are
         *               silently ignored
         * @param serializer the file-body serializer that turns a per-file
         *                   entity list into the new UTF-8 JSON text
         * @return this builder
         * @throws IllegalStateException if two distinct sources stage the
         *         same file path, which indicates a manifest schema bug
         */
        public @NotNull Builder add(
            @NotNull StagedBatch<?> staged,
            @NotNull FileSerializer serializer
        ) {
            if (staged.isEmpty())
                return this;

            for (java.util.Map.Entry<String, ? extends dev.simplified.collection.ConcurrentList<?>> entry : staged.getFileSnapshots().entrySet()) {
                String path = entry.getKey();

                if (this.fileContents.containsKey(path))
                    throw new IllegalStateException(
                        "Duplicate staged file path '" + path + "' across sources - manifest schema bug?"
                    );

                String body = serializer.serialize(entry.getValue());
                this.fileContents.put(path, body);
            }

            this.sourceBatches.add(staged);
            this.totalMutationCount += staged.getMutationCount();
            return this;
        }

        /** @return the fully accumulated request. */
        public @NotNull BatchCommitRequest build() {
            return new BatchCommitRequest(
                this.fileContents.toUnmodifiable(),
                this.sourceBatches.toUnmodifiable(),
                this.totalMutationCount
            );
        }

    }

    /**
     * Strategy that converts a per-file entity list into a UTF-8 JSON body.
     * The scheduler passes in a Gson-backed implementation that honors the
     * entity type's existing JSON shape so the committed bytes round-trip
     * cleanly on the next read cycle.
     */
    @FunctionalInterface
    public interface FileSerializer {

        @NotNull String serialize(@NotNull ConcurrentList<?> entities);

    }

}
