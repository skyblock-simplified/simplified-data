package dev.sbs.simplifieddata.persistence;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import dev.sbs.simplifieddata.write.BufferedMutation;
import dev.simplified.client.exception.PreconditionFailedException;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import dev.simplified.persistence.source.MutableSource;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

/**
 * A {@link MutableSource} that wraps a read-path {@link Source} chain and layers
 * GitHub Contents API write-back on top of it.
 *
 * <p>The read path is fully delegated to the injected {@code delegate} source -
 * the Phase 5 {@code DiskOverlaySource(RemoteJsonSource(...))} chain, or any
 * other {@link Source} impl for tests. {@link #load(JpaRepository)} calls
 * through unchanged, so reads still respect the disk overlay layer.
 *
 * <p>The write path is completely independent of the delegate. Producers call
 * {@link #upsert(JpaModel)} or {@link #delete(JpaModel)} which append to an
 * in-memory per-entity-id buffer and return immediately - no network I/O,
 * no GitHub call. The {@link #commitBatch()} method is invoked by the
 * {@code WriteBatchScheduler} every ~10s to drain the buffer and push all
 * accumulated mutations to GitHub via a single
 * {@code PUT /repos/.../contents/{path}} call per dirty file.
 *
 * <p>Because every {@code WritableRemoteJsonSource} instance handles exactly
 * one entity type (and one entity type maps to exactly one primary manifest
 * entry), the "group by file" step inside {@code commitBatch()} is trivial -
 * the source resolves its own target path via {@link IndexProvider#loadIndex()}
 * (cheap due to the framework's auto-304 revalidation cache) and issues a
 * single PUT per non-empty buffer tick.
 *
 * <p><b>Phase 6b known limitation: {@code _extra} companion files are ignored
 * on the write path.</b> Entities currently in a companion file (e.g. an Item
 * living in {@code items_extra.json}) get promoted to the primary file
 * ({@code items.json}) on first mutation because the write path always targets
 * the primary path from the manifest entry. The promotion emits an INFO log so
 * operators can see the routing decision. Keeping the companion path in sync
 * is a Phase 6e follow-up that would require either reading both files and
 * dispatching by id, or teaching the manifest generator to merge companions
 * back into the primary on every regeneration.
 *
 * <p>Optimistic concurrency on the PUT is handled via the blob SHA carried by
 * {@link GitHubContentEnvelope#getSha()}. On a {@code 412 Precondition Failed}
 * (surfaced as {@link PreconditionFailedException}) the source refetches the
 * blob SHA and retries the PUT up to {@code max412ImmediateRetries} times
 * before escalating the offending mutations back to the caller via a returned
 * {@link CommitBatchResult}. The caller (the scheduler or consumer) owns the
 * exponential-backoff retry loop - this class only reports which
 * {@link BufferedMutation} entries failed to commit.
 *
 * <p>Thread safety: the buffer is a {@link ConcurrentMap} so
 * {@link #upsert(JpaModel)} / {@link #delete(JpaModel)} are safe to call from
 * any thread (specifically the {@code WriteQueueConsumer} daemon thread). The
 * {@link #commitBatch()} call is NOT reentrant within the same source - the
 * scheduler invokes it serially from a single {@code scheduling-1} thread and
 * the source does not guard against concurrent commit ticks. If the scheduler
 * is ever parallelized, a {@code ReentrantLock} should be added.
 *
 * @param <T> the entity type this source writes
 * @see SkyBlockDataWriteContract
 * @see BufferedMutation
 * @see WriteRequest.Operation
 */
@Getter
@Log4j2
public class WritableRemoteJsonSource<T extends JpaModel> implements MutableSource<T> {

    /**
     * Maximum immediate retries on a {@code 412 Precondition Failed} before
     * escalating the failed mutations to the caller's exponential-backoff path.
     * Each retry refetches the current blob SHA and re-PUTs with the fresh token.
     */
    private static final int DEFAULT_MAX_412_IMMEDIATE_RETRIES = 3;

    /** The read-path source chain. Reads delegate to it unchanged. */
    private final @NotNull Source<T> delegate;

    /** The Phase 6b GitHub write-path contract (Contents API JSON media type). */
    private final @NotNull SkyBlockDataWriteContract writeContract;

    /**
     * The manifest index provider used to resolve the target file path at
     * commit time. Shared with the read-path {@code RemoteJsonSource} so the
     * framework's auto-304 revalidation cache means lookups after the first
     * one cost zero rate budget.
     */
    private final @NotNull IndexProvider indexProvider;

    /** The Gson instance used to serialize the mutated entity list back to JSON. */
    private final @NotNull Gson gson;

    /** The human-readable source id. Matches {@code ExternalAssetState.sourceId} and the write path logs. */
    private final @NotNull String sourceId;

    /** The entity class this source handles. Used for manifest entry lookup and Gson TypeToken resolution. */
    private final @NotNull Class<T> modelClass;

    /** Maximum 412 immediate retries per commit tick. */
    private final int max412ImmediateRetries;

    /**
     * The in-memory buffer of pending mutations, keyed by the entity's @Id
     * field value rendered as a string. Upserts and deletes for the same id
     * overwrite each other within a tick, matching the semantics of a
     * last-write-wins queue.
     */
    private final @NotNull ConcurrentMap<String, BufferedMutation<T>> buffer;

    /**
     * Cached accessor for the entity's {@code @Id} field. Resolved once at
     * construction and kept as a field to avoid repeated reflection scans.
     */
    private final @NotNull FieldAccessor<?> idAccessor;

    /**
     * Constructs a new writable remote source with default retry settings.
     *
     * @param delegate the read-path source chain (typically
     *                 {@code DiskOverlaySource(RemoteJsonSource(...))})
     * @param writeContract the GitHub Contents API write-path contract
     * @param indexProvider the manifest index provider
     * @param gson the Gson instance for list serialization
     * @param sourceId the human-readable source id
     * @param modelClass the entity class
     */
    public WritableRemoteJsonSource(
        @NotNull Source<T> delegate,
        @NotNull SkyBlockDataWriteContract writeContract,
        @NotNull IndexProvider indexProvider,
        @NotNull Gson gson,
        @NotNull String sourceId,
        @NotNull Class<T> modelClass
    ) {
        this(delegate, writeContract, indexProvider, gson, sourceId, modelClass, DEFAULT_MAX_412_IMMEDIATE_RETRIES);
    }

    /**
     * Constructs a new writable remote source with a custom
     * {@code max412ImmediateRetries} cap. Production wires this via the
     * {@code skyblock.data.github.write-412-immediate-retries} property.
     *
     * @param delegate the read-path source chain
     * @param writeContract the GitHub Contents API write-path contract
     * @param indexProvider the manifest index provider
     * @param gson the Gson instance for list serialization
     * @param sourceId the human-readable source id
     * @param modelClass the entity class
     * @param max412ImmediateRetries maximum immediate 412 retries before escalation
     */
    public WritableRemoteJsonSource(
        @NotNull Source<T> delegate,
        @NotNull SkyBlockDataWriteContract writeContract,
        @NotNull IndexProvider indexProvider,
        @NotNull Gson gson,
        @NotNull String sourceId,
        @NotNull Class<T> modelClass,
        int max412ImmediateRetries
    ) {
        this.delegate = delegate;
        this.writeContract = writeContract;
        this.indexProvider = indexProvider;
        this.gson = gson;
        this.sourceId = sourceId;
        this.modelClass = modelClass;
        this.max412ImmediateRetries = max412ImmediateRetries;
        this.buffer = Concurrent.newMap();
        this.idAccessor = new Reflection<>(modelClass).getFields()
            .stream()
            .filter(fa -> fa.hasAnnotation(Id.class))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No @Id field found on entity class '" + modelClass.getName() + "' - cannot compute buffer key"
            ));
    }

    @Override
    public @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException {
        return this.delegate.load(repository);
    }

    /**
     * Buffers an upsert for the given entity. Does NOT touch GitHub.
     *
     * <p>The entity's id is resolved via the cached {@code @Id} accessor and
     * rendered as a string to key the buffer. Subsequent upserts or deletes
     * with the same id overwrite the previous entry within a tick.
     *
     * @param entity the entity to persist
     * @return the same entity instance (no origin-side normalization since
     *         the mutation is buffered, not applied)
     * @throws JpaException if the entity has no resolvable {@code @Id} value
     */
    @Override
    public @NotNull T upsert(@NotNull T entity) throws JpaException {
        String id = this.requireIdString(entity);
        this.buffer.put(id, new BufferedMutation<>(WriteRequest.Operation.UPSERT, entity, java.util.UUID.randomUUID(), Instant.now()));
        log.debug("buffered upsert on source '{}' type '{}' id='{}'", this.sourceId, this.modelClass.getSimpleName(), id);
        return entity;
    }

    /**
     * Buffers a delete for the given entity. Does NOT touch GitHub.
     *
     * @param entity the entity to remove
     * @return the same entity instance
     * @throws JpaException if the entity has no resolvable {@code @Id} value
     */
    @Override
    public @NotNull T delete(@NotNull T entity) throws JpaException {
        String id = this.requireIdString(entity);
        this.buffer.put(id, new BufferedMutation<>(WriteRequest.Operation.DELETE, entity, java.util.UUID.randomUUID(), Instant.now()));
        log.debug("buffered delete on source '{}' type '{}' id='{}'", this.sourceId, this.modelClass.getSimpleName(), id);
        return entity;
    }

    /**
     * Buffers a mutation produced by a {@link WriteQueueConsumer} dispatch.
     *
     * <p>This overload carries the originating {@link WriteRequest#getRequestId()}
     * so the buffered entry can trace back to the producer's request id in logs
     * and in the dead-letter IMap. Test code uses the plain
     * {@link #upsert(JpaModel)} / {@link #delete(JpaModel)} entry points which
     * generate a fresh request id per call.
     *
     * @param mutation the mutation to buffer
     * @throws JpaException if the entity has no resolvable {@code @Id} value
     */
    public void buffer(@NotNull BufferedMutation<T> mutation) throws JpaException {
        String id = this.requireIdString(mutation.getEntity());
        this.buffer.put(id, mutation);
        log.debug(
            "buffered {} on source '{}' type '{}' id='{}' requestId={}",
            mutation.getOperation(), this.sourceId, this.modelClass.getSimpleName(), id, mutation.getRequestId()
        );
    }

    /**
     * Snapshots and drains the buffer, then commits the accumulated mutations
     * to GitHub in a single {@code PUT /contents/{path}} call.
     *
     * <p>Work sequence:
     * <ol>
     *   <li>Atomically snapshot the buffer into a local copy and clear the
     *       underlying buffer so the next tick can accept fresh mutations.</li>
     *   <li>If the snapshot is empty, return {@link CommitBatchResult#empty()}
     *       immediately.</li>
     *   <li>Resolve the target file path via {@link IndexProvider#loadIndex()}
     *       and find the manifest entry whose {@code modelClass} equals this
     *       source's entity FQCN.</li>
     *   <li>GET the current envelope via
     *       {@link SkyBlockDataWriteContract#getFileMetadata(String)} to read
     *       the current blob SHA + the current file content.</li>
     *   <li>Decode the current content (base64 or raw UTF-8 depending on size)
     *       into a {@code ConcurrentList<T>} via Gson.</li>
     *   <li>Apply the mutations: upserts replace by id, deletes remove by id.</li>
     *   <li>Serialize the mutated list back to JSON, base64-encode, and PUT it
     *       with the observed blob SHA as the optimistic-concurrency token.</li>
     *   <li>On {@link PreconditionFailedException}, refetch the blob SHA and
     *       retry up to {@link #max412ImmediateRetries} times.</li>
     *   <li>On any other failure (including a hard 412 after all immediate
     *       retries are exhausted), return all buffered mutations in
     *       {@link CommitBatchResult#getFailures()} so the caller can escalate
     *       to the exponential-backoff retry path.</li>
     * </ol>
     *
     * @return a {@link CommitBatchResult} summarizing the tick: the number of
     *         applied mutations, the new commit SHA (if the PUT succeeded),
     *         and any failed mutations that must be re-queued by the caller
     */
    public @NotNull CommitBatchResult commitBatch() {
        ConcurrentMap<String, BufferedMutation<T>> snapshot = this.drainBuffer();

        if (snapshot.isEmpty())
            return CommitBatchResult.empty();

        try {
            return this.commitSnapshot(snapshot);
        } catch (Throwable ex) {
            log.error(
                "commitBatch failed for source '{}' type '{}' with {} mutations - escalating all to retry path",
                this.sourceId, this.modelClass.getSimpleName(), snapshot.size(), ex
            );
            return CommitBatchResult.failed(snapshot.values(), ex);
        }
    }

    /**
     * Internal commit loop: resolve path, fetch envelope, decode body, apply
     * mutations, PUT, retry on 412.
     */
    private @NotNull CommitBatchResult commitSnapshot(@NotNull ConcurrentMap<String, BufferedMutation<T>> snapshot) throws JpaException, SkyBlockDataException {
        String filePath = this.resolveTargetPath();

        int attempt = 0;
        PreconditionFailedException lastPrecondition = null;

        while (attempt <= this.max412ImmediateRetries) {
            GitHubContentEnvelope envelope;

            try {
                envelope = this.writeContract.getFileMetadata(filePath);
            } catch (SkyBlockDataException ex) {
                throw new JpaException(ex, "Failed to fetch metadata for '%s' from source '%s'", filePath, this.sourceId);
            }

            ConcurrentList<T> currentEntities = this.decodeCurrentEntities(envelope, filePath);
            ConcurrentList<T> mutated = this.applyMutations(currentEntities, snapshot);

            String newJson = this.gson.toJson(mutated, this.listType());
            String newContent = Base64.getEncoder().encodeToString(newJson.getBytes(StandardCharsets.UTF_8));
            String message = this.buildCommitMessage(snapshot);

            PutContentRequest body = PutContentRequest.builder()
                .message(message)
                .content(newContent)
                .sha(envelope.getSha())
                .branch("master")
                .build();

            try {
                GitHubPutResponse response = this.writeContract.putFileContent(filePath, body);
                log.info(
                    "committed {} {} mutation(s) to '{}' on source '{}' (blobSha='{}' commitSha='{}' attempt={})",
                    snapshot.size(), this.modelClass.getSimpleName(), filePath, this.sourceId,
                    response.getContent().getSha(), response.getCommit().getSha(), attempt
                );
                return CommitBatchResult.success(snapshot.size(), response.getCommit().getSha());
            } catch (PreconditionFailedException ex) {
                lastPrecondition = ex;
                attempt++;
                log.warn(
                    "412 Precondition Failed on PUT '{}' for source '{}' (attempt {}/{}) - refetching blob SHA",
                    filePath, this.sourceId, attempt, this.max412ImmediateRetries
                );
            } catch (SkyBlockDataException ex) {
                throw new JpaException(
                    ex,
                    "Failed to PUT '%s' on source '%s' (HTTP %d)",
                    filePath, this.sourceId, ex.getStatus().getCode()
                );
            }
        }

        log.error(
            "Exhausted {} immediate 412 retries on PUT '{}' for source '{}' - escalating {} mutations to backoff retry path",
            this.max412ImmediateRetries, filePath, this.sourceId, snapshot.size()
        );
        return CommitBatchResult.failed(snapshot.values(), lastPrecondition);
    }

    /**
     * Atomically drains the buffer and returns the snapshot. Any mutations
     * that arrive after this call land in the next tick's buffer.
     */
    private @NotNull ConcurrentMap<String, BufferedMutation<T>> drainBuffer() {
        ConcurrentMap<String, BufferedMutation<T>> snapshot = Concurrent.newMap();
        for (java.util.Map.Entry<String, BufferedMutation<T>> entry : this.buffer.entrySet())
            snapshot.put(entry.getKey(), entry.getValue());
        for (String key : snapshot.keySet())
            this.buffer.remove(key);
        return snapshot;
    }

    /**
     * Resolves the target file path for this source's entity class by looking
     * up the matching manifest entry and returning its {@code path} field.
     */
    private @NotNull String resolveTargetPath() throws JpaException {
        ManifestIndex manifest = this.indexProvider.loadIndex();
        ManifestIndex.Entry entry = manifest.getFiles()
            .stream()
            .filter(e -> e.getModelClass().equals(this.modelClass.getName()))
            .findFirst()
            .orElseThrow(() -> new JpaException(
                "No manifest entry for '%s' under source '%s' - cannot resolve write path",
                this.modelClass.getName(), this.sourceId
            ));
        return entry.getPath();
    }

    /**
     * Decodes the current file body from the envelope's base64 content field,
     * or fetches the raw content if the envelope reports size over the 1 MB
     * base64 limit (the envelope's content field is empty for large files;
     * the write-path contract does not have a raw read method, so this
     * condition is a hard error that operators must resolve by activating
     * the Git Data API path in Phase 6e).
     */
    private @NotNull ConcurrentList<T> decodeCurrentEntities(
        @NotNull GitHubContentEnvelope envelope,
        @NotNull String filePath
    ) throws JpaException {
        if (envelope.getContent().isEmpty()) {
            throw new JpaException(
                "File '%s' on source '%s' is too large for the Contents API base64 envelope "
                    + "(size=%d bytes) - write path requires Phase 6e Git Data API activation",
                filePath, this.sourceId, envelope.getSize()
            );
        }

        String base64 = envelope.getContent().replace("\n", "").replace("\r", "");
        byte[] decoded = Base64.getDecoder().decode(base64);
        String body = new String(decoded, StandardCharsets.UTF_8);

        ConcurrentList<T> loaded = this.gson.fromJson(body, this.listType());
        return loaded != null ? loaded : Concurrent.newList();
    }

    /**
     * Applies the buffered mutations to the current entity list in place.
     * Upserts replace existing entries by id or append if absent; deletes
     * remove by id.
     */
    private @NotNull ConcurrentList<T> applyMutations(
        @NotNull ConcurrentList<T> currentEntities,
        @NotNull ConcurrentMap<String, BufferedMutation<T>> snapshot
    ) throws JpaException {
        ConcurrentList<T> result = Concurrent.newList();
        ConcurrentMap<String, T> byId = Concurrent.newMap();

        for (T existing : currentEntities) {
            String id = this.requireIdString(existing);
            byId.put(id, existing);
            result.add(existing);
        }

        for (ConcurrentMap.Entry<String, BufferedMutation<T>> entry : snapshot.entrySet()) {
            String id = entry.getKey();
            BufferedMutation<T> mutation = entry.getValue();

            switch (mutation.getOperation()) {
                case UPSERT -> {
                    T existing = byId.get(id);
                    if (existing != null) {
                        int index = result.indexOf(existing);
                        result.set(index, mutation.getEntity());
                        byId.put(id, mutation.getEntity());
                    } else {
                        result.add(mutation.getEntity());
                        byId.put(id, mutation.getEntity());
                    }
                }
                case DELETE -> {
                    T existing = byId.get(id);
                    if (existing != null) {
                        result.remove(existing);
                        byId.remove(id);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Builds the commit message for this batch tick. Per Q4 (simplified for
     * Option A): {@code "Update <ClassName>: <N> mutation(s)"} header with
     * one line per entry in the body.
     */
    private @NotNull String buildCommitMessage(@NotNull ConcurrentMap<String, BufferedMutation<T>> snapshot) {
        int count = snapshot.size();
        StringBuilder sb = new StringBuilder();
        sb.append("Update ").append(this.modelClass.getSimpleName()).append(": ").append(count)
            .append(" mutation").append(count == 1 ? "" : "s").append("\n\n");

        for (ConcurrentMap.Entry<String, BufferedMutation<T>> entry : snapshot.entrySet()) {
            sb.append("- ")
                .append(entry.getValue().getOperation())
                .append(" ")
                .append(this.modelClass.getSimpleName())
                .append(" id=")
                .append(entry.getKey())
                .append("\n");
        }

        return sb.toString();
    }

    /** Reflection helper: resolves the {@code @Id} field value of an entity to a non-null string. */
    private @NotNull String requireIdString(@NotNull T entity) throws JpaException {
        Object id = this.idAccessor.get(entity);

        if (id == null)
            throw new JpaException(
                "Entity of type '%s' has null @Id field '%s' - cannot buffer mutation",
                this.modelClass.getName(), this.idAccessor.getName()
            );

        return Objects.toString(id);
    }

    /** Gson TypeToken for the entity list type. */
    private @NotNull Type listType() {
        return TypeToken.getParameterized(ConcurrentList.class, this.modelClass).getType();
    }

    /**
     * Result of a single {@link #commitBatch()} tick.
     *
     * <p>On success, carries the applied mutation count and the new commit
     * SHA. On failure, carries the list of buffered mutations the caller
     * must escalate to the exponential-backoff retry path.
     */
    @Getter
    public static final class CommitBatchResult {

        /** Empty result produced when the source's buffer was empty at drain time. */
        private static final @NotNull CommitBatchResult EMPTY = new CommitBatchResult(0, null, Concurrent.newUnmodifiableList(), null);

        private final int appliedCount;
        private final @Nullable String commitSha;
        private final @NotNull ConcurrentList<BufferedMutation<?>> failures;
        private final @Nullable Throwable failureCause;

        private CommitBatchResult(
            int appliedCount,
            @Nullable String commitSha,
            @NotNull ConcurrentList<BufferedMutation<?>> failures,
            @Nullable Throwable failureCause
        ) {
            this.appliedCount = appliedCount;
            this.commitSha = commitSha;
            this.failures = failures;
            this.failureCause = failureCause;
        }

        /** @return {@code true} when the batch committed successfully (no failures). */
        public boolean isSuccess() {
            return this.failures.isEmpty() && this.failureCause == null;
        }

        /** @return {@code true} when the tick drained an empty buffer. */
        public boolean isEmpty() {
            return this.appliedCount == 0 && this.failures.isEmpty();
        }

        /**
         * Result sentinel for an empty drain tick. Public for test
         * construction; production code accesses it via
         * {@link WritableRemoteJsonSource#commitBatch}.
         */
        public static @NotNull CommitBatchResult empty() {
            return EMPTY;
        }

        /**
         * Success factory carrying the applied count + new commit SHA.
         * Public for test construction; production code constructs this
         * inside {@link WritableRemoteJsonSource#commitSnapshot}.
         *
         * @param appliedCount the number of mutations that landed on GitHub
         * @param commitSha the new commit SHA produced by the PUT response
         * @return a success-state result
         */
        public static @NotNull CommitBatchResult success(int appliedCount, @NotNull String commitSha) {
            return new CommitBatchResult(appliedCount, commitSha, Concurrent.newUnmodifiableList(), null);
        }

        /**
         * Failure factory carrying the list of buffered mutations the
         * caller must re-queue. Public for test construction.
         *
         * @param failures the mutations that failed to commit
         * @param cause the root cause exception for log output
         * @return a failed-state result
         */
        public static @NotNull CommitBatchResult failed(
            @NotNull java.util.Collection<? extends BufferedMutation<?>> failures,
            @Nullable Throwable cause
        ) {
            ConcurrentList<BufferedMutation<?>> copy = Concurrent.newList();
            copy.addAll(failures);
            return new CommitBatchResult(0, null, copy.toUnmodifiableList(), cause);
        }

    }

}
