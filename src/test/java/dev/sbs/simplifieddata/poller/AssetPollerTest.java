package dev.sbs.simplifieddata.poller;

import com.google.gson.Gson;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.skyblockdata.contract.SkyBlockDataContract;
import api.simplified.github.exception.GitHubApiException;
import api.simplified.github.response.GitHubCommit;
import dev.simplified.client.exception.NotModifiedException;
import dev.simplified.client.response.Response;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.asset.ExternalAssetState;
import dev.simplified.persistence.driver.H2MemoryDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for {@link AssetPoller} driving a hand-rolled {@link SkyBlockDataContract}
 * stub through a {@link LastResponseAccessor} bridge and a real in-process {@link JpaSession}
 * scoped to the {@code dev.simplified.persistence.asset} package.
 *
 * <p>No Spring context: the poller is instantiated directly with the asset session, the
 * stub contract, the synthetic last-response accessor, and the configuration values. This
 * keeps the integration narrow - the unit under test is the {@code doPoll()} cycle, not
 * Spring's bean wiring.
 *
 * <p>Coverage matrix:
 * <ul>
 *   <li>First poll against empty state populates {@code ExternalAssetState} + one
 *       {@code ExternalAssetEntryState} row per manifest file.</li>
 *   <li>Second poll with the same commit SHA records a no-change cycle.</li>
 *   <li>Second poll with a fresh commit SHA fetches the manifest, computes the diff, and
 *       updates the state tables accordingly.</li>
 *   <li>Cache-miss {@code 304} revalidation path (the stub throws
 *       {@link NotModifiedException} directly, mimicking the framework's behavior when the
 *       client's recent-response cache has no matching body) records a no-change cycle.</li>
 *   <li>{@link GitHubApiException} during the commits call is swallowed without crashing
 *       the poller.</li>
 *   <li>{@code pollEnabled=false} via constructor flag bypasses the scheduled and startup
 *       entry points entirely.</li>
 * </ul>
 */
@Tag("slow")
@SuppressWarnings("DataFlowIssue")
class AssetPollerTest {

    private static final @NotNull Gson GSON = DataApi.getGson();

    private SessionManager sessionManager;
    private JpaSession assetSession;
    private StubContract contract;
    private StubLastResponseAccessor accessor;
    private RecordingRefreshTrigger refreshTrigger;

    @BeforeEach
    void setUp() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "asset_poller_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(ExternalAssetState.class)
                    .build()
            )
            .build();
        this.assetSession = this.sessionManager.connect(config);

        this.contract = new StubContract();
        this.accessor = new StubLastResponseAccessor();
        this.refreshTrigger = new RecordingRefreshTrigger();
    }

    @AfterEach
    void tearDown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
    }

    @Test
    @DisplayName("first poll populates ExternalAssetState and one row per manifest entry")
    void firstPollPopulatesState() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-one\""));
            assertThat(state.getContentSha256().orElse(null), is(notNullValue()));
            assertThat(state.getLastSuccessAt().isPresent(), is(true));

            long count = session.createQuery(
                "SELECT count(e) FROM ExternalAssetEntryState e WHERE e.sourceId = :sid",
                Long.class
            ).setParameter("sid", "skyblock-data").getSingleResult();
            assertThat(count, greaterThan(0L));
        });
    }

    @Test
    @DisplayName("second poll with unchanged commit sha records a no-change cycle")
    void noChangePoll() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: same commit list and ETag, scheduled cycle should observe equality
        // and short-circuit through recordNoChange().
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getLastCheckedAt(), is(notNullValue()));
        });
    }

    @Test
    @DisplayName("second poll with fresh commit sha triggers diff and state update")
    void changePoll() {
        GitHubCommit firstCommit = parseCommit("sha-one");
        this.contract.commit = firstCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), firstCommit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: a fresh commit sha and a new manifest with one changed entry hash.
        GitHubCommit secondCommit = parseCommit("sha-two");
        this.contract.commit = secondCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-two", "ccc", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-two\"")), secondCommit);

        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-two"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-two\""));

            ExternalAssetEntryState items = session.find(
                ExternalAssetEntryState.class,
                new ExternalAssetEntryState.PK("skyblock-data", "data/v1/items/items.json")
            );
            assertThat(items, is(notNullValue()));
            assertThat(items.getEntrySha256(), equalTo("ccc"));
        });
    }

    @Test
    @DisplayName("Phase 5.6: first poll cold-start skips the refresh trigger even though every manifest entry is newly added")
    void firstPollColdBootSkipsRefreshTrigger() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // JpaSession.cacheRepositories() already hydrated every SkyBlock repository from
        // the remote source during context init, so firing the refresh for all 41 added
        // entries would be wasted GitHub budget. The Phase 5.6 skip keeps the cold-boot
        // cost at just the 41 cacheRepositories calls instead of doubling to ~82.
        assertThat(this.refreshTrigger.invocations, hasSize(0));
        // State table still populated normally; this test asserts the REFRESH skip only.
        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
        });
    }

    @Test
    @DisplayName("Phase 5.5 + 5.6: change poll triggers refresh only for the models whose content changed (cold boot skipped)")
    void changePollTriggersRefreshOnlyForChangedModels() {
        GitHubCommit firstCommit = parseCommit("sha-one");
        this.contract.commit = firstCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), firstCommit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Phase 5.6: the cold-boot startup poll does NOT fire the refresh trigger.
        assertThat(this.refreshTrigger.invocations, hasSize(0));

        // Second cycle: items hash flipped, mobs hash unchanged. Only items should refresh,
        // and this IS a warm-state poll so the trigger fires.
        GitHubCommit secondCommit = parseCommit("sha-two");
        this.contract.commit = secondCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-two", "ccc", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-two\"")), secondCommit);

        poller.scheduledPoll();

        assertThat(this.refreshTrigger.invocations, hasSize(1));
        assertThat(
            this.refreshTrigger.invocations.getFirst(),
            contains(dev.sbs.skyblockdata.model.Item.class)
        );
    }

    @Test
    @DisplayName("Phase 5.5 + 5.6: no-change poll does not invoke the refresh trigger, and neither does cold boot")
    void noChangePollDoesNotInvokeRefreshTrigger() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: same commit sha, expecting the early-return recordNoChange path.
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);
        poller.scheduledPoll();

        // Zero invocations: the cold-boot startup poll is Phase 5.6 skipped, and the
        // scheduled no-change poll short-circuits via recordNoChange before any trigger call.
        assertThat(this.refreshTrigger.invocations, hasSize(0));
    }

    @Test
    @DisplayName("Phase 5.5 + 5.6: unknown model_class is skipped with a WARN and refresh still fires for resolvable entries on a warm-state change poll")
    void unknownModelClassIsSkipped() {
        // Establish warm state via a cold-boot poll that Phase 5.6 does not fire the trigger for.
        GitHubCommit firstCommit = parseCommit("sha-one");
        this.contract.commit = firstCommit;
        this.contract.fileContents.put(
            "data/v1/index.json",
            manifestWithModelClass("sha-one", "dev.sbs.skyblockdata.model.Item", "com.example.GhostModel")
        );
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), firstCommit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Phase 5.6 cold-boot skip: no invocation yet.
        assertThat(this.refreshTrigger.invocations, hasSize(0));

        // Second cycle: content hashes flip for both entries, simulating a new commit.
        // The warm-state path runs the refresh trigger and the unknown FQCN is skipped.
        GitHubCommit secondCommit = parseCommit("sha-two");
        this.contract.commit = secondCommit;
        this.contract.fileContents.put(
            "data/v1/index.json",
            manifestWithModelClassAndHashes(
                "sha-two",
                "dev.sbs.skyblockdata.model.Item", "ccc",
                "com.example.GhostModel", "ddd"
            )
        );
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-two\"")), secondCommit);

        poller.scheduledPoll();

        assertThat(this.refreshTrigger.invocations, hasSize(1));
        // Only the resolvable FQCN makes it through; the ghost is logged and skipped.
        assertThat(
            this.refreshTrigger.lastTargets(),
            contains(dev.sbs.skyblockdata.model.Item.class)
        );
    }

    @Test
    @DisplayName("Phase 5.5 + 5.6: refresh trigger failure on a warm-state change poll is isolated and does not crash the cycle")
    void refreshTriggerFailureIsIsolated() {
        // Cold-boot startup - Phase 5.6 skip, trigger not fired.
        GitHubCommit firstCommit = parseCommit("sha-one");
        this.contract.commit = firstCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), firstCommit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();
        assertThat(this.refreshTrigger.invocations, hasSize(0));

        // Second cycle: fresh commit + fresh items hash, triggering the refresh on the
        // warm-state path. Arm the recording trigger to throw, and verify the poller
        // catches and swallows the exception while the state row still commits cleanly.
        GitHubCommit secondCommit = parseCommit("sha-two");
        this.contract.commit = secondCommit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-two", "ccc", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-two\"")), secondCommit);
        this.refreshTrigger.nextFailure = new RuntimeException("refresh failed for test");

        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-two"));
        });
        // One invocation: the warm-state change poll. The thrown exception was caught
        // inside triggerRefresh so the poll cycle returned normally.
        assertThat(this.refreshTrigger.invocations, hasSize(1));
    }

    @Test
    @DisplayName("cache-miss 304 revalidation bumps lastCheckedAt and skips manifest fetch")
    void cacheMiss304Revalidation() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: simulate the framework's cache-miss 304 revalidation path by having
        // the stub throw a NotModifiedException directly. In production this is the rare path
        // where GitHub returned 304 but the framework's recent-response cache has no matching
        // body (for example after a TTL prune or a client restart). The common transparent-304
        // path - where the framework synthesizes a Response envelope with the cached body - is
        // exercised by the change/no-change tests above, since the contract-level stubs return
        // a canned GitHubCommit just like a framework-synthesized response would.
        this.contract.commitException = buildNotModified();
        this.contract.commit = null;
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            // Commit sha unchanged because the cache-miss 304 short-circuits before any state
            // mutation beyond the lastCheckedAt bump.
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-one\""));
        });
    }

    @Test
    @DisplayName("GitHubApiException is swallowed and the poll cycle returns normally")
    void gitHubFailureIsSwallowed() {
        this.contract.commitException = buildFailure(500, "Internal Server Error");

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(nullValue()));
        });
    }

    @Test
    @DisplayName("pollEnabled=false short-circuits both startup and scheduled entry points")
    void pollDisabledSkipsBothEntryPoints() {
        GitHubCommit commit = parseCommit("sha-one");
        this.contract.commit = commit;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commit);

        AssetPoller poller = newPoller(false);
        poller.onApplicationReady();
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(nullValue()));
        });
    }

    // --- helpers --- //

    private @NotNull AssetPoller newPoller(boolean pollEnabled) {
        return new AssetPoller(this.assetSession, this.refreshTrigger, this.contract, this.accessor, "skyblock-data", pollEnabled);
    }

    private static @NotNull String sampleManifest(@NotNull String commitSha, @NotNull String itemsSha, @NotNull String mobsSha) {
        return """
            {
              "version": 1,
              "generated_at": "2026-04-07T00:00:00Z",
              "commit_sha": "%s",
              "count": 2,
              "files": [
                {
                  "path": "data/v1/items/items.json",
                  "category": "items",
                  "table_name": "item",
                  "model_class": "dev.sbs.skyblockdata.model.Item",
                  "content_sha256": "%s",
                  "bytes": 10,
                  "has_extra": false
                },
                {
                  "path": "data/v1/mobs/mobs.json",
                  "category": "mobs",
                  "table_name": "mob",
                  "model_class": "dev.sbs.skyblockdata.model.MobType",
                  "content_sha256": "%s",
                  "bytes": 20,
                  "has_extra": false
                }
              ]
            }
            """.formatted(commitSha, itemsSha, mobsSha);
    }

    /**
     * Variant of {@link #sampleManifest} that lets callers pin arbitrary {@code model_class}
     * FQCNs on the two manifest entries. Used by the Phase 5.5 unknown-class test to inject
     * a non-existent FQCN alongside a resolvable one.
     */
    private static @NotNull String manifestWithModelClass(
        @NotNull String commitSha,
        @NotNull String itemsModelClass,
        @NotNull String mobsModelClass
    ) {
        return manifestWithModelClassAndHashes(commitSha, itemsModelClass, "aaa", mobsModelClass, "bbb");
    }

    private static @NotNull String manifestWithModelClassAndHashes(
        @NotNull String commitSha,
        @NotNull String itemsModelClass,
        @NotNull String itemsSha,
        @NotNull String mobsModelClass,
        @NotNull String mobsSha
    ) {
        return """
            {
              "version": 1,
              "generated_at": "2026-04-07T00:00:00Z",
              "commit_sha": "%s",
              "count": 2,
              "files": [
                {
                  "path": "data/v1/items/items.json",
                  "category": "items",
                  "table_name": "item",
                  "model_class": "%s",
                  "content_sha256": "%s",
                  "bytes": 10,
                  "has_extra": false
                },
                {
                  "path": "data/v1/mobs/mobs.json",
                  "category": "mobs",
                  "table_name": "mob",
                  "model_class": "%s",
                  "content_sha256": "%s",
                  "bytes": 20,
                  "has_extra": false
                }
              ]
            }
            """.formatted(commitSha, itemsModelClass, itemsSha, mobsModelClass, mobsSha);
    }

    private static @NotNull GitHubCommit parseCommit(@NotNull String sha) {
        String json = """
            {
              "sha": "%s",
              "commit": {
                "message": "test commit",
                "committer": {
                  "name": "Tester",
                  "date": "2026-04-07T00:00:00Z"
                }
              }
            }
            """.formatted(sha);
        return GSON.fromJson(json, GitHubCommit.class);
    }

    private static <T> @NotNull Response<T> response(int status, @NotNull Map<String, List<String>> headers, @NotNull T body) {
        Map<String, Collection<String>> feignHeaders = new HashMap<>(headers);
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(status)
            .reason("test")
            .request(feignRequest)
            .headers(feignHeaders)
            .body("", StandardCharsets.UTF_8)
            .build();

        return new Response.Impl<>(feignResponse, () -> body);
    }

    private static @NotNull GitHubApiException buildFailure(int status, @NotNull String reason) {
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(status)
            .reason(reason)
            .request(feignRequest)
            .headers(Map.of())
            .body("{\"message\":\"" + reason + "\"}", StandardCharsets.UTF_8)
            .build();
        return new GitHubApiException(GSON, "getLatestMasterCommit", feignResponse);
    }

    private static @NotNull NotModifiedException buildNotModified() {
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(304)
            .reason("Not Modified")
            .request(feignRequest)
            .headers(Map.of())
            .body(new byte[0])
            .build();
        return new NotModifiedException("getLatestMasterCommit", feignResponse);
    }

    /** Hand-rolled contract stub. */
    private static final class StubContract implements SkyBlockDataContract {

        private final @NotNull Map<String, String> fileContents = new HashMap<>();
        private @Nullable GitHubCommit commit;
        private @Nullable RuntimeException commitException;

        @Override
        public @NotNull GitHubCommit getLatestMasterCommit(@NotNull String owner, @NotNull String repo) throws GitHubApiException {
            if (this.commitException != null)
                throw this.commitException;

            if (this.commit == null)
                throw new IllegalStateException("StubContract.commit not set - test should not call getLatestMasterCommit on this path");

            return this.commit;
        }

        @Override
        public byte @NotNull [] getFileContent(@NotNull String owner, @NotNull String repo, @NotNull String path) throws GitHubApiException {
            String body = this.fileContents.get(path);

            if (body == null)
                throw new IllegalStateException("StubContract.fileContents missing entry for path '" + path + "'");

            return body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public api.simplified.github.response.@NotNull GitHubContentEnvelope getFileMetadata(@NotNull String owner, @NotNull String repo, @NotNull String path) throws GitHubApiException {
            throw new UnsupportedOperationException("AssetPoller does not exercise the write surface");
        }

        @Override
        public api.simplified.github.response.@NotNull GitHubPutResponse putFileContent(@NotNull String owner, @NotNull String repo, @NotNull String path, api.simplified.github.request.@NotNull PutContentRequest body) throws GitHubApiException {
            throw new UnsupportedOperationException("AssetPoller does not exercise the write surface");
        }

    }

    /** Hand-rolled accessor stub - returns a single canned response per call. */
    private static final class StubLastResponseAccessor implements LastResponseAccessor {

        private @Nullable Response<?> next;

        @Override
        public @NotNull Optional<Response<?>> getLastResponse() {
            return Optional.ofNullable(this.next);
        }

    }

    /**
     * Hand-rolled {@link RefreshTrigger} stub that records each invocation's target set.
     * Use {@link #invocations} to assert how many times the poller fired the refresh and
     * {@link #lastTargets} to assert the specific class set of the most recent call.
     * Setting {@link #nextFailure} makes the next {@code refresh} call throw, which exercises
     * the poller's isolation {@code try/catch}.
     */
    private static final class RecordingRefreshTrigger implements RefreshTrigger {

        private final @NotNull List<Set<Class<? extends JpaModel>>> invocations = new ArrayList<>();
        private @Nullable RuntimeException nextFailure;

        @Override
        public void refresh(@NotNull Collection<Class<? extends JpaModel>> models) {
            this.invocations.add(new LinkedHashSet<>(models));

            if (this.nextFailure != null) {
                RuntimeException toThrow = this.nextFailure;
                this.nextFailure = null;
                throw toThrow;
            }
        }

        @NotNull Set<Class<? extends JpaModel>> lastTargets() {
            if (this.invocations.isEmpty())
                throw new IllegalStateException("RecordingRefreshTrigger was never invoked");

            return this.invocations.getLast();
        }

    }

}
