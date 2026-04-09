package dev.sbs.simplifieddata.write;

import dev.sbs.simplifieddata.client.SkyBlockGitDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.CreateBlobRequest;
import dev.sbs.simplifieddata.client.request.CreateCommitRequest;
import dev.sbs.simplifieddata.client.request.CreateTreeRequest;
import dev.sbs.simplifieddata.client.request.UpdateRefRequest;
import dev.sbs.simplifieddata.client.response.GitBlob;
import dev.sbs.simplifieddata.client.response.GitCommit;
import dev.sbs.simplifieddata.client.response.GitRef;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Phase 6b.1 Git Data API commit orchestrator. Turns a
 * {@link BatchCommitRequest} (cross-source aggregated file map) into a single
 * atomic GitHub commit on {@code skyblock-data/master} via the 7-step Git
 * Data API flow:
 * <ol>
 *   <li>{@code GET /git/refs/heads/master} &rarr; current branch tip SHA
 *       (via {@link SkyBlockGitDataContract#getRef(String)})</li>
 *   <li>{@code GET /git/commits/{sha}} &rarr; current tree SHA from the tip
 *       commit (via {@link SkyBlockGitDataContract#getCommit(String)})</li>
 *   <li>{@code POST /git/blobs} &times; N &rarr; one blob per dirty file in
 *       the request (via {@link SkyBlockGitDataContract#createBlob(CreateBlobRequest)})</li>
 *   <li>{@code POST /git/trees} with {@code base_tree} pointing at the
 *       current tree and {@code tree} entries for every new blob (via
 *       {@link SkyBlockGitDataContract#createTree(CreateTreeRequest)})</li>
 *   <li>{@code POST /git/commits} &rarr; new commit referencing the new tree
 *       and the current tip commit as parent (via
 *       {@link SkyBlockGitDataContract#createCommit(CreateCommitRequest)})</li>
 *   <li>{@code PATCH /git/refs/heads/master} with {@code force: false}
 *       &rarr; moves the branch pointer to the new commit if it is a
 *       fast-forward (via
 *       {@link SkyBlockGitDataContract#updateRef(String, UpdateRefRequest)})</li>
 * </ol>
 *
 * <p>Retry policy (Q4 locked answer): up to 3 immediate retries from step 1
 * if any step raises a {@link SkyBlockDataException} with HTTP status in the
 * retryable set (409 Conflict, 422 Unprocessable Entity - the typical
 * symptoms of a concurrent writer landing a competing commit during our
 * flow). After 3 retries are exhausted, the service returns a
 * {@link GitDataCommitResult#failure(Throwable) failure} envelope carrying
 * the root cause; the {@link WriteBatchScheduler} escalates every mutation
 * in the request's source batches to the consumer's exponential-backoff
 * retry queue. Any other {@link Throwable} (network, 5xx, 403 permissions)
 * skips immediate retries and escalates straight to the backoff path.
 *
 * <p>Thread safety: the service is a Spring singleton but every invocation
 * is a fresh 7-step flow with no shared mutable state. The
 * {@link SkyBlockGitDataContract} proxy is thread-safe via Feign's default
 * implementation, so multiple concurrent
 * {@link #commit(BatchCommitRequest)} calls are safe. In practice the
 * {@link WriteBatchScheduler} only invokes this from its single scheduling
 * thread.
 *
 * @see WriteBatchScheduler
 * @see SkyBlockGitDataContract
 */
@Component
@Log4j2
public class GitDataCommitService {

    /** The branch name targeted by the Git Data API flow. */
    public static final @NotNull String TARGET_BRANCH = "master";

    /** Git mode for a regular file in a tree entry. */
    private static final @NotNull String BLOB_MODE = "100644";

    /** Git type marker for a file blob in a tree entry. */
    private static final @NotNull String BLOB_TYPE = "blob";

    /** Content encoding for {@link CreateBlobRequest} - UTF-8 plaintext since our files are JSON text. */
    private static final @NotNull String BLOB_ENCODING = "utf-8";

    private final @NotNull SkyBlockGitDataContract contract;
    private final @NotNull WriteMetrics metrics;
    private final int maxImmediateRetries;

    public GitDataCommitService(
        @NotNull SkyBlockGitDataContract skyBlockGitDataContract,
        @NotNull WriteMetrics writeMetrics,
        @Value("${skyblock.data.github.write-412-immediate-retries:3}") int maxImmediateRetries
    ) {
        this.contract = skyBlockGitDataContract;
        this.metrics = writeMetrics;
        this.maxImmediateRetries = maxImmediateRetries;
    }

    /**
     * Commits every file in the supplied {@link BatchCommitRequest} as a
     * single atomic GitHub commit via the Git Data API.
     *
     * <p>An empty request returns {@link GitDataCommitResult#success(String)}
     * with an empty commit SHA and does not touch the network. This matches
     * the contract of {@link WritableRemoteJsonSource#commitBatch()} for
     * ticks where every source's buffer was empty.
     *
     * @param request the aggregated cross-source staging result
     * @return the commit outcome
     */
    public @NotNull GitDataCommitResult commit(@NotNull BatchCommitRequest request) {
        if (request.isEmpty()) {
            log.debug("commit() called with empty BatchCommitRequest - no-op");
            return GitDataCommitResult.success("");
        }

        int attempt = 0;
        Throwable lastCause = null;

        while (attempt <= this.maxImmediateRetries) {
            try {
                String commitSha = this.runFlow(request);
                log.info(
                    "Git Data commit succeeded: {} mutation(s) across {} file(s) -> commit '{}' (attempt={})",
                    request.getTotalMutationCount(), request.getDirtyFileCount(), commitSha, attempt
                );
                return GitDataCommitResult.success(commitSha);
            } catch (SkyBlockDataException ex) {
                lastCause = ex;
                int status = ex.getStatus().getCode();

                if (isRetryable(status) && attempt < this.maxImmediateRetries) {
                    attempt++;
                    log.warn(
                        "Git Data commit attempt {} failed with status {} - retrying from getRef (max {})",
                        attempt, status, this.maxImmediateRetries
                    );
                    continue;
                }

                // Retryable status but immediate-retry budget exhausted, OR a non-retryable
                // status on the first attempt. Only the budget-exhausted case qualifies for
                // the "retries exhausted" meter per the Phase 6b.3 catalog semantics.
                if (isRetryable(status))
                    this.metrics.recordRetriesExhausted(WriteMetrics.CommitMode.GIT_DATA);

                log.error(
                    "Git Data commit failed with status {} on attempt {} (retryable={}) - escalating to backoff",
                    status, attempt, isRetryable(status), ex
                );
                break;
            } catch (Throwable ex) {
                lastCause = ex;
                log.error(
                    "Git Data commit threw unexpected exception on attempt {} - escalating to backoff",
                    attempt, ex
                );
                break;
            }
        }

        return GitDataCommitResult.failure(lastCause != null ? lastCause : new IllegalStateException("commit failed without cause"));
    }

    /**
     * Executes one pass of the 7-step Git Data API flow. Throws on any
     * non-2xx response; the caller's retry loop decides whether to retry
     * based on the exception's HTTP status.
     */
    private @NotNull String runFlow(@NotNull BatchCommitRequest request) throws SkyBlockDataException {
        // Step 1: get the current ref tip.
        io.micrometer.core.instrument.Timer.Sample getRefSample = this.metrics.recordGitDataStepStart();
        GitRef currentRef = this.contract.getRef(TARGET_BRANCH);
        this.metrics.recordGitDataStepEnd(getRefSample, WriteMetrics.GitDataStep.GET_REF);
        String parentCommitSha = currentRef.getObject().getSha();

        // Step 2: get the current tree SHA from the parent commit.
        io.micrometer.core.instrument.Timer.Sample getCommitSample = this.metrics.recordGitDataStepStart();
        GitCommit parentCommit = this.contract.getCommit(parentCommitSha);
        this.metrics.recordGitDataStepEnd(getCommitSample, WriteMetrics.GitDataStep.GET_COMMIT);
        String baseTreeSha = parentCommit.getTree().getSha();

        // Steps 3..N: create one blob per dirty file, collect tree entries.
        // Each createBlob call is timed individually so the step_duration histogram
        // reflects per-blob latency rather than the summed latency of all blobs in a batch.
        ConcurrentList<CreateTreeRequest.TreeEntry> treeEntries = Concurrent.newList();

        for (Map.Entry<String, String> fileEntry : request.getFileContents().entrySet()) {
            String path = fileEntry.getKey();
            String content = fileEntry.getValue();

            CreateBlobRequest blobRequest = CreateBlobRequest.builder()
                .content(content)
                .encoding(BLOB_ENCODING)
                .build();
            io.micrometer.core.instrument.Timer.Sample blobSample = this.metrics.recordGitDataStepStart();
            GitBlob blob = this.contract.createBlob(blobRequest);
            this.metrics.recordGitDataStepEnd(blobSample, WriteMetrics.GitDataStep.CREATE_BLOB);

            CreateTreeRequest.TreeEntry treeEntry = CreateTreeRequest.TreeEntry.builder()
                .path(path)
                .mode(BLOB_MODE)
                .type(BLOB_TYPE)
                .sha(blob.getSha())
                .build();
            treeEntries.add(treeEntry);
        }

        // Step N+1: create a tree layered on the parent's tree with the new blob entries.
        CreateTreeRequest treeRequest = CreateTreeRequest.builder()
            .baseTree(baseTreeSha)
            .tree(treeEntries)
            .build();
        io.micrometer.core.instrument.Timer.Sample treeSample = this.metrics.recordGitDataStepStart();
        var newTree = this.contract.createTree(treeRequest);
        this.metrics.recordGitDataStepEnd(treeSample, WriteMetrics.GitDataStep.CREATE_TREE);

        // Step N+2: create the commit referencing the new tree and the parent commit.
        CreateCommitRequest commitRequest = CreateCommitRequest.builder()
            .message(buildCommitMessage(request))
            .tree(newTree.getSha())
            .parents(Concurrent.newList(parentCommitSha))
            .build();
        io.micrometer.core.instrument.Timer.Sample commitSample = this.metrics.recordGitDataStepStart();
        GitCommit newCommit = this.contract.createCommit(commitRequest);
        this.metrics.recordGitDataStepEnd(commitSample, WriteMetrics.GitDataStep.CREATE_COMMIT);

        // Step N+3: update the branch ref to the new commit. force=false enforces fast-forward.
        UpdateRefRequest updateRequest = UpdateRefRequest.builder()
            .sha(newCommit.getSha())
            .force(false)
            .build();
        io.micrometer.core.instrument.Timer.Sample updateRefSample = this.metrics.recordGitDataStepStart();
        this.contract.updateRef(TARGET_BRANCH, updateRequest);
        this.metrics.recordGitDataStepEnd(updateRefSample, WriteMetrics.GitDataStep.UPDATE_REF);

        return newCommit.getSha();
    }

    /**
     * Builds the Git Data API commit message. Follows the locked Q4 format
     * now that coalescing across files in a single commit is achievable:
     * {@code "Batch update: <N> entities across <M> files"} header with
     * per-source per-entity-class breakdown in the body.
     */
    private static @NotNull String buildCommitMessage(@NotNull BatchCommitRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("Batch update: ")
            .append(request.getTotalMutationCount())
            .append(" entities across ")
            .append(request.getDirtyFileCount())
            .append(" files\n\n");

        for (StagedBatch<?> sourceBatch : request.getSourceBatches()) {
            if (sourceBatch.isEmpty())
                continue;

            sb.append("- ")
                .append(sourceBatch.getModelClass().getSimpleName())
                .append(": ")
                .append(sourceBatch.getMutationCount())
                .append(" mutation")
                .append(sourceBatch.getMutationCount() == 1 ? "" : "s")
                .append("\n");
        }

        return sb.toString();
    }

    /**
     * Determines whether a {@link SkyBlockDataException} HTTP status should
     * trigger an immediate retry from step 1. {@code 409 Conflict} and
     * {@code 422 Unprocessable Entity} are the two statuses a concurrent
     * writer can produce mid-flow; all other statuses escalate to the
     * exponential-backoff path.
     */
    private static boolean isRetryable(int status) {
        return status == 409 || status == 422;
    }

}
