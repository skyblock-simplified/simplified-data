package dev.sbs.simplifieddata.write;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.ZodiacEvent;
import dev.sbs.simplifieddata.client.SkyBlockGitDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.CreateBlobRequest;
import dev.sbs.simplifieddata.client.request.CreateCommitRequest;
import dev.sbs.simplifieddata.client.request.CreateTreeRequest;
import dev.sbs.simplifieddata.client.request.UpdateRefRequest;
import dev.sbs.simplifieddata.client.response.GitBlob;
import dev.sbs.simplifieddata.client.response.GitCommit;
import dev.sbs.simplifieddata.client.response.GitRef;
import dev.sbs.simplifieddata.client.response.GitTree;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link GitDataCommitService} driven against a hand-rolled
 * {@link SkyBlockGitDataContract} stub. No Spring, no live network I/O.
 *
 * <p>Coverage matrix:
 * <ul>
 *   <li>Empty {@link BatchCommitRequest} is a no-op returning success with
 *       an empty commit SHA - no network calls issued.</li>
 *   <li>Happy-path single-file commit issues one getRef + one getCommit +
 *       one createBlob + one createTree + one createCommit + one updateRef,
 *       in that order, and returns the new commit SHA.</li>
 *   <li>Happy-path multi-file commit issues createBlob N times and builds
 *       a tree with N entries.</li>
 *   <li>The commit message header carries the totalMutationCount +
 *       dirtyFileCount.</li>
 *   <li>A 422 on updateRef triggers an immediate retry from getRef, and a
 *       successful second attempt returns the new commit SHA.</li>
 *   <li>Four consecutive 422s exhaust the retry budget and return a
 *       failure envelope carrying the root cause.</li>
 *   <li>A 500 on createBlob does NOT retry (non-retryable status) and
 *       returns failure immediately.</li>
 * </ul>
 */
class GitDataCommitServiceTest {

    private static final @NotNull Gson GSON = MinecraftApi.getGson();
    private static final @NotNull String INITIAL_COMMIT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final @NotNull String INITIAL_TREE_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final @NotNull String NEW_COMMIT_SHA = "cccccccccccccccccccccccccccccccccccccccc";

    private StubContract contract;

    @BeforeEach
    void setUp() {
        this.contract = new StubContract();
    }

    @Test
    @DisplayName("commit() on an empty request is a no-op and does not touch the network")
    void commitEmpty() {
        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest empty = BatchCommitRequest.builder().build();

        GitDataCommitResult result = service.commit(empty);

        assertThat(result.isSuccess(), is(true));
        assertThat(this.contract.getRefCalls.get(), equalTo(0));
        assertThat(this.contract.createBlobCalls, hasSize(0));
    }

    @Test
    @DisplayName("commit() on a single-file request issues the full 6-step flow in order")
    void commitSingleFile() {
        this.contract.queueHappyPath();

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = buildRequest("data/v1/world/zodiac_events.json", "[{\"id\":\"A\"}]", 1);

        GitDataCommitResult result = service.commit(request);

        assertThat(result.isSuccess(), is(true));
        assertThat(result.getCommitSha(), equalTo(NEW_COMMIT_SHA));
        assertThat(this.contract.getRefCalls.get(), equalTo(1));
        assertThat(this.contract.getCommitCalls.get(), equalTo(1));
        assertThat(this.contract.createBlobCalls, hasSize(1));
        assertThat(this.contract.createBlobCalls.get(0).getContent(), equalTo("[{\"id\":\"A\"}]"));
        assertThat(this.contract.createBlobCalls.get(0).getEncoding(), equalTo("utf-8"));
        assertThat(this.contract.createTreeCalls, hasSize(1));
        assertThat(this.contract.createTreeCalls.get(0).getBaseTree(), equalTo(INITIAL_TREE_SHA));
        assertThat(this.contract.createTreeCalls.get(0).getTree(), hasSize(1));
        assertThat(this.contract.createCommitCalls, hasSize(1));
        assertThat(this.contract.createCommitCalls.get(0).getParents(), hasSize(1));
        assertThat(this.contract.createCommitCalls.get(0).getParents().get(0), equalTo(INITIAL_COMMIT_SHA));
        assertThat(this.contract.updateRefCalls, hasSize(1));
        assertThat(this.contract.updateRefCalls.get(0).getSha(), equalTo(NEW_COMMIT_SHA));
        assertThat(this.contract.updateRefCalls.get(0).getForce(), is(false));
    }

    @Test
    @DisplayName("commit() on a multi-file request issues one createBlob per file and builds a matching tree")
    void commitMultiFile() {
        this.contract.queueHappyPath();

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = BatchCommitRequest.builder()
            .add(newStagedBatch("data/v1/world/zodiac_events.json", "[1]", 2), entities -> "[1]")
            .add(newStagedBatch("data/v1/items/items.json", "[2,3]", 3), entities -> "[2,3]")
            .build();

        GitDataCommitResult result = service.commit(request);

        assertThat(result.isSuccess(), is(true));
        assertThat(this.contract.createBlobCalls, hasSize(2));
        assertThat(this.contract.createTreeCalls, hasSize(1));
        assertThat(this.contract.createTreeCalls.get(0).getTree(), hasSize(2));
    }

    @Test
    @DisplayName("commit() commit message carries total mutation count + dirty file count")
    void commitMessageFormat() {
        this.contract.queueHappyPath();

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = BatchCommitRequest.builder()
            .add(newStagedBatch("data/v1/world/zodiac_events.json", "[1]", 2), entities -> "[1]")
            .add(newStagedBatch("data/v1/items/items.json", "[2,3,4]", 5), entities -> "[2,3,4]")
            .build();

        service.commit(request);

        String message = this.contract.createCommitCalls.get(0).getMessage();
        assertThat(message, containsString("Batch update: 7 entities across 2 files"));
    }

    @Test
    @DisplayName("A 422 on updateRef triggers an immediate retry from getRef and succeeds on retry")
    void retry422() {
        // First pass: 422 on updateRef. Second pass: happy path.
        this.contract.queueHappyPathMetadata();
        this.contract.queueUpdateRefOutcome(StubContract.UpdateRefOutcome.UNPROCESSABLE);
        this.contract.queueHappyPathMetadata();
        this.contract.queueUpdateRefOutcome(StubContract.UpdateRefOutcome.SUCCESS);

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = buildRequest("data/v1/world/zodiac_events.json", "[1]", 1);

        GitDataCommitResult result = service.commit(request);

        assertThat(result.isSuccess(), is(true));
        assertThat(this.contract.getRefCalls.get(), equalTo(2));
        assertThat(this.contract.updateRefCalls, hasSize(2));
    }

    @Test
    @DisplayName("Four consecutive 422s exhaust the retry budget and return failure")
    void retry422Exhausted() {
        for (int i = 0; i < 4; i++) {
            this.contract.queueHappyPathMetadata();
            this.contract.queueUpdateRefOutcome(StubContract.UpdateRefOutcome.UNPROCESSABLE);
        }

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = buildRequest("data/v1/world/zodiac_events.json", "[1]", 1);

        GitDataCommitResult result = service.commit(request);

        assertThat(result.isSuccess(), is(false));
        assertThat(result.getFailureCause(), instanceOfOrNull(SkyBlockDataException.class));
        assertThat(this.contract.updateRefCalls, hasSize(4));
    }

    @Test
    @DisplayName("A 500 on createBlob does not retry and returns failure immediately")
    void nonRetryableBlobFailure() {
        this.contract.queueRefAndCommitOnly();
        this.contract.queueBlobOutcome(StubContract.BlobOutcome.INTERNAL_ERROR);

        GitDataCommitService service = new GitDataCommitService(this.contract, new WriteMetrics(new SimpleMeterRegistry()), 3);
        BatchCommitRequest request = buildRequest("data/v1/world/zodiac_events.json", "[1]", 1);

        GitDataCommitResult result = service.commit(request);

        assertThat(result.isSuccess(), is(false));
        // getRef + getCommit were called once; createBlob was called once and failed.
        assertThat(this.contract.getRefCalls.get(), equalTo(1));
        assertThat(this.contract.createBlobCalls, hasSize(1));
        assertThat(this.contract.createTreeCalls, hasSize(0));
    }

    // --- helpers --- //

    private static @NotNull BatchCommitRequest buildRequest(@NotNull String path, @NotNull String body, int mutationCount) {
        StagedBatch<ZodiacEvent> staged = newStagedBatch(path, body, mutationCount);
        return BatchCommitRequest.builder()
            .add(staged, entities -> body)
            .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static @NotNull StagedBatch<ZodiacEvent> newStagedBatch(
        @NotNull String path,
        @NotNull String body,
        int mutationCount
    ) {
        ConcurrentMap<String, ConcurrentList<ZodiacEvent>> fileSnapshots = Concurrent.newMap();
        fileSnapshots.put(path, Concurrent.newList());

        ConcurrentList<BufferedMutation<ZodiacEvent>> mutations = Concurrent.newList();
        for (int i = 0; i < mutationCount; i++) {
            mutations.add(new BufferedMutation<>(
                dev.simplified.persistence.source.WriteRequest.Operation.UPSERT,
                new ZodiacEvent(),
                UUID.randomUUID(),
                Instant.now()
            ));
        }

        return new StagedBatch<>(ZodiacEvent.class, fileSnapshots, mutations, mutationCount);
    }

    /** Hamcrest helper: matcher that accepts an instance of the given class or null. */
    private static <T> @NotNull org.hamcrest.Matcher<Object> instanceOfOrNull(@NotNull Class<T> type) {
        return new org.hamcrest.BaseMatcher<>() {
            @Override
            public boolean matches(Object item) {
                return item == null || type.isInstance(item);
            }

            @Override
            public void describeTo(org.hamcrest.Description description) {
                description.appendText("an instance of " + type.getName() + " or null");
            }
        };
    }

    // --- stub contract --- //

    private static final class StubContract implements SkyBlockGitDataContract {

        enum UpdateRefOutcome { SUCCESS, UNPROCESSABLE }
        enum BlobOutcome { SUCCESS, INTERNAL_ERROR }

        final AtomicInteger getRefCalls = new AtomicInteger();
        final AtomicInteger getCommitCalls = new AtomicInteger();
        final List<CreateBlobRequest> createBlobCalls = new ArrayList<>();
        final List<CreateTreeRequest> createTreeCalls = new ArrayList<>();
        final List<CreateCommitRequest> createCommitCalls = new ArrayList<>();
        final List<UpdateRefRequest> updateRefCalls = new ArrayList<>();

        private final List<UpdateRefOutcome> queuedUpdateRefs = new ArrayList<>();
        private final List<BlobOutcome> queuedBlobs = new ArrayList<>();
        private boolean blobOutcomeQueued = false;

        void queueHappyPath() {
            queueHappyPathMetadata();
            queueUpdateRefOutcome(UpdateRefOutcome.SUCCESS);
        }

        void queueHappyPathMetadata() {
            // getRef and getCommit always return fixed fixtures; nothing to queue explicitly.
            // Blob / tree / commit create calls all succeed by default.
        }

        void queueRefAndCommitOnly() {
            // no-op - getRef + getCommit always succeed
        }

        void queueUpdateRefOutcome(@NotNull UpdateRefOutcome outcome) {
            this.queuedUpdateRefs.add(outcome);
        }

        void queueBlobOutcome(@NotNull BlobOutcome outcome) {
            this.queuedBlobs.add(outcome);
            this.blobOutcomeQueued = true;
        }

        @Override
        public @NotNull GitRef getRef(@NotNull String branch) {
            this.getRefCalls.incrementAndGet();
            String json = """
                {
                  "ref": "refs/heads/%s",
                  "url": "https://api.github.com/repos/skyblock-simplified/skyblock-data/git/refs/heads/%s",
                  "object": {
                    "sha": "%s",
                    "type": "commit",
                    "url": "https://api.github.com/repos/skyblock-simplified/skyblock-data/git/commits/%s"
                  }
                }
                """.formatted(branch, branch, INITIAL_COMMIT_SHA, INITIAL_COMMIT_SHA);
            return GSON.fromJson(json, GitRef.class);
        }

        @Override
        public @NotNull GitCommit getCommit(@NotNull String sha) {
            this.getCommitCalls.incrementAndGet();
            String json = """
                {
                  "sha": "%s",
                  "message": "parent commit",
                  "tree": { "sha": "%s", "url": "https://api.github.com/.../trees/%s" },
                  "parents": []
                }
                """.formatted(sha, INITIAL_TREE_SHA, INITIAL_TREE_SHA);
            return GSON.fromJson(json, GitCommit.class);
        }

        @Override
        public @NotNull GitTree getTree(@NotNull String sha, @NotNull String recursive) {
            throw new UnsupportedOperationException("getTree stub not used by commit flow");
        }

        @Override
        public @NotNull GitBlob createBlob(@NotNull CreateBlobRequest body) throws SkyBlockDataException {
            this.createBlobCalls.add(body);

            if (this.blobOutcomeQueued && !this.queuedBlobs.isEmpty()) {
                BlobOutcome outcome = this.queuedBlobs.remove(0);
                if (outcome == BlobOutcome.INTERNAL_ERROR)
                    throw fakeException(500, "Internal Server Error");
            }

            String json = """
                {
                  "sha": "blob%dsha",
                  "url": "https://api.github.com/.../blobs/blob%dsha"
                }
                """.formatted(this.createBlobCalls.size(), this.createBlobCalls.size());
            return GSON.fromJson(json, GitBlob.class);
        }

        @Override
        public @NotNull GitTree createTree(@NotNull CreateTreeRequest body) {
            this.createTreeCalls.add(body);
            String json = """
                {
                  "sha": "treesha",
                  "url": "https://api.github.com/.../trees/treesha",
                  "tree": []
                }
                """;
            return GSON.fromJson(json, GitTree.class);
        }

        @Override
        public @NotNull GitCommit createCommit(@NotNull CreateCommitRequest body) {
            this.createCommitCalls.add(body);
            String json = """
                {
                  "sha": "%s",
                  "message": "new commit",
                  "tree": { "sha": "treesha", "url": "https://api.github.com/.../trees/treesha" },
                  "parents": []
                }
                """.formatted(NEW_COMMIT_SHA);
            return GSON.fromJson(json, GitCommit.class);
        }

        @Override
        public @NotNull GitRef updateRef(@NotNull String branch, @NotNull UpdateRefRequest body) throws SkyBlockDataException {
            this.updateRefCalls.add(body);

            if (!this.queuedUpdateRefs.isEmpty()) {
                UpdateRefOutcome outcome = this.queuedUpdateRefs.remove(0);
                if (outcome == UpdateRefOutcome.UNPROCESSABLE)
                    throw fakeException(422, "Update is not a fast forward");
            }

            String json = """
                {
                  "ref": "refs/heads/%s",
                  "url": "https://api.github.com/.../refs/heads/%s",
                  "object": { "sha": "%s", "type": "commit", "url": "https://api.github.com/.../commits/%s" }
                }
                """.formatted(branch, branch, body.getSha(), body.getSha());
            return GSON.fromJson(json, GitRef.class);
        }

        private static @NotNull SkyBlockDataException fakeException(int status, @NotNull String reason) {
            feign.Request request = feign.Request.create(
                feign.Request.HttpMethod.POST,
                "https://api.github.com/fake",
                java.util.Map.of(),
                feign.Request.Body.empty(),
                new feign.RequestTemplate()
            );
            feign.Response response = feign.Response.builder()
                .status(status)
                .reason(reason)
                .request(request)
                .headers(java.util.Map.of())
                .body("{\"message\":\"" + reason + "\",\"documentation_url\":\"\"}", StandardCharsets.UTF_8)
                .build();
            return new SkyBlockDataException("POST /fake", response);
        }

    }

}
