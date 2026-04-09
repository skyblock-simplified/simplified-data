# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See the root [`CLAUDE.md`](../CLAUDE.md) for cross-cutting patterns, dependency details, and the
locked [simplified-data + Hazelcast initiative](../../../.claude/projects/W--Workspace-Java-SkyBlock-Simplified/memory/architecture_simplified_data_initiative.md)
in the auto-memory.

## Build & Test

```bash
# From repo root
./gradlew :simplified-data:build          # Build (includes shadowJar)
./gradlew :simplified-data:test           # Run all tests

# Spring Boot context test requires a live Hazelcast cluster on skyblock-hazelcast-net
# Skip with SKYBLOCK_HAZELCAST_DISABLED=true in environments where the cluster is unavailable.
SKYBLOCK_HAZELCAST_DISABLED=true ./gradlew :simplified-data:test

# Fat JAR
./gradlew :simplified-data:shadowJar      # Output: build/libs/simplified-data-0.1.0.jar
```

## Module Overview

`simplified-data` is the autonomous data writer service for the SkyBlock-Simplified initiative.
Phase 2c scope is **scaffolding only**: a Spring Boot context that wires `JpaCacheProvider.HAZELCAST_CLIENT`
against the docker cluster defined in `infra/hazelcast/`. Later phases add the IQueue write consumer,
GitHub asset polling, and the skyblock-data repo integration.

### Phase scope tracker

| Phase | Status | Scope |
|---|---|---|
| 2c | done | Spring Boot scaffold + Hazelcast client wiring + L2 cache validation |
| 2d | done | Lazy-streaming JpaRepository rewrite, query cache disabled under Hazelcast |
| 3 | done | skyblock-data repo populated (42 JSONs, 41 entities, manifest generator, CI) |
| 4a | done | Library-side Source interface foundation (Simplified-Dev/persistence) |
| 4b | done | GitHub integration: SkyBlockDataContract + GitHubIndexProvider + GitHubFileFetcher + GitHubConfig |
| 4c | done | Scheduled AssetPoller (watchdog-only) + AssetDiffEngine + dedicated asset-state JpaSession |
| 5 | done | Switch SkyBlock repositories to RemoteSkyBlockFactory (DiskOverlaySource + RemoteJsonSource) |
| 5.5 | done | AssetPoller targeted refresh trigger via `RefreshTrigger` SAM + `JpaSession.refreshModels` library API. Changes propagate on the next 60s poll instead of waiting for restart. |
| 5.5.1 | done | Deleted obsolete Phase 4b ETagContext workaround after `Simplified-Dev/client` auto-attaches `If-None-Match` and transparently serves cached bodies on 304. Net -160 lines across GitHubConfig, AssetPoller, SkyBlockDataException, AssetPollerTest. |
| 5.5.2 | done | Hotfix: switch `SkyBlockDataContract.getLatestMasterCommit` from `/commits?sha=master&per_page=1` (stale edge cache, observed 10+ min lag) to `/commits/master` (always fresh via git ref path). Return type collapsed to single `GitHubCommit`. Phase 5.5 gate-6 end-to-end verified live against docker. |
| 6a | done | `WriteRequest` envelope class + 9 unit tests in `Simplified-Dev/persistence`. Plain Java `Serializable`, pre-serialized `entityJson`, `UPSERT`/`DELETE` nested enum. Library foundation for the IQueue write path. |
| 6b | done | **Write path wiring**: `WritableRemoteJsonSource<T>` wraps the `DiskOverlaySource(RemoteJsonSource)` chain and layers GitHub Contents API PUT on top. `SkyBlockDataWriteContract` + new DTOs + separate `skyBlockDataWriteClient` bean with `Accept: application/vnd.github+json`. `WriteQueueConsumer` daemon thread drains `skyblock.writes` IQueue + a local `DelayQueue<DelayedWriteRequest>` for exponential-backoff retries; dead-letter to `skyblock.writes.deadletter` IMap after 5 attempts. `WriteBatchScheduler` fires every 10s and escalates failures to the consumer's retry queue. `skyBlockWriteHazelcastInstance` bean in `PersistenceConfig` (second client alongside the JCache-managed instance). `RemoteSkyBlockFactory` wraps every per-model source in a `WritableRemoteJsonSource` and exposes a typed `getWritableSources()` registry for the write-path beans. `SmokeWriteSentinel` bean activated under `@Profile("smoke")` for gate-7 end-to-end docker testing. Dormant `SkyBlockGitDataContract` + 8 Git Data API DTOs shipped as an extractable API surface with compile-only DTO round-trip tests - no production caller in Phase 6b, reserved for Phase 6b.1. |
| 6b.3 | done | **Micrometer meters for the write path**. New `WriteMetrics` `@Component` in `write/` that owns every meter exposed by `simplified-data`: 8 counters, 4 timers, 4 gauges. Bounded-enum tag types (`CommitMode`, `SkipReason`, `FailureReason`, `GitDataStep`) enforce cardinality discipline. Per-type buffer depth gauges bounded at 41 (one per SkyBlock entity), dead-letter counter tagged by FQCN (same bound), no request ids in any tag. `WriteMetricsTest` covers every helper method via Micrometer's in-memory `SimpleMeterRegistry`. **Dependency shape change**: dropped `spring-boot-starter` + `spring-boot-starter-actuator` in favour of `server-api` which transitively exports `spring-boot-starter-web` and `spring-boot-starter-actuator` via `api()` coordinates, matching `simplified-server`. Added `micrometer-registry-prometheus` as an explicit catalog dep pinned to 1.14.5. Flipped `spring.main.web-application-type` from `none` to `servlet` so the `/actuator/prometheus` scrape endpoint is reachable on port 8080 (private to `skyblock-hazelcast-net`, never published to host). Disabled `api.key.authentication.enabled` because simplified-data has no REST endpoints to protect. Spring Boot version bumped 3.4.4 -> 3.4.5 to match `server-api`'s catalog. `SimplifiedData` main class now scans `dev.sbs.serverapi` in addition to `dev.sbs.simplifieddata` so the framework's auto-config beans register. Wired `WriteMetrics` into `WriteQueueConsumer` (fresh/retry dispatch counters, dispatch latency timer, skip/dead-letter counters, 3 Hazelcast-backed gauges registered in `start()`), `WriteBatchScheduler` (commit duration timer + end-to-end latency per mutation + escalation counter, both commit modes), `GitDataCommitService` (per-step Timer.Sample around each of the 6 Git Data API steps + retries-exhausted counter on budget exhaustion), `WritableRemoteJsonSource` (per-type buffer depth gauge registered in the constructor), `RemoteSkyBlockFactory` (threads `WriteMetrics` through the per-model `buildSourceFor` helper), and `PersistenceConfig` (injects `WriteMetrics` into the `remoteSkyBlockFactory` bean method). |
| 6b.2 | done | **WriteRequest.withRequestId hygiene cleanup**: drop the reflection-based `rebuildWithRequestId` static helper in `WriteBatchScheduler` (unwrapped the private `WriteRequest` constructor to preserve the original producer's request id across retry cycles) in favour of a new library-side `WriteRequest.withRequestId(UUID)` instance method on `Simplified-Dev/persistence`. Net -55 lines on the service side, +37 lines (library method + unit test) on the library side. Runtime behavior byte-identical - same UUID substitution, same field preservation, just via a proper library API instead of reflection on `getDeclaredConstructor`. |
| 6b.1 | done | **Git Data API production path + dual-file routing + gap fixes**: `WritableRemoteJsonSource.stageBatch()` new two-phase commit entry point that reads both primary and `_extra` files via `FileFetcher`, routes mutations to the owning file by id (extras wins on UPSERT, DELETE removes from BOTH files on conflict, new ids default to primary), and returns a `StagedBatch` with only the files that actually changed (suppresses byte-identical no-ops). `GitDataCommitService` (@Component) orchestrates the 7-step Git Data API flow (getRef → getCommit → createBlob*N → createTree(base_tree) → createCommit → updateRef) with 3 immediate retries on 409/422 non-fast-forward before escalating. `WriteBatchScheduler.tick()` dispatches on the `skyblock.data.github.write-mode` property: `GIT_DATA` (default) uses a two-phase staging + single-commit path that produces exactly one commit per tick across every dirty file on every source; `CONTENTS` retains the Phase 6b per-file commit path as an operational fallback. Commit message in GIT_DATA mode matches the original Q4 format: `"Batch update: <N> entities across <M> files"` header with per-entity-class breakdown in the body. Two known-gap fixes landed after the initial 6b.1 commits: (1) **DELETE bug** for cross-file id conflicts now removes from both primary and extras files (previously only removed from extras, leaving the stale primary visible via the read-path append merge). (2) **Durable retry state** migrated from in-process `DelayQueue<DelayedWriteRequest>` to Hazelcast `IMap<UUID, RetryEnvelope> skyblock.writes.retry` so in-flight exponential-backoff retries survive consumer crash/restart. The migration also fixes a latent attempt-counter bug where `escalateStagedBatch` hardcoded `attempt=1` on every escalation - the new path reads `mutation.getAttempt()` (new field on `BufferedMutation`) and correctly computes `nextAttempt = current + 1`, producing a bounded retry chain that terminates at `maxRetryAttempts` instead of looping forever. Fixes the items_extra.json routing bug that Phase 6b had flagged as a known limitation. |
| 6c | future | simplified-bot producer SDK (`WriteDispatcher` service bean) - speculatively ship without concrete write triggers |
| 6d | future | Operator runbook: `SKYBLOCK_DATA_GITHUB_TOKEN` upgraded to `contents:write` scope |
| 7 | future | Delete the 42 resource JSONs from `minecraft-api/src/main/resources/skyblock/` |

### Entry Point

- **`SimplifiedData`** - Spring Boot application. Headless (no web server). Bootstraps a `JpaSession`
  via `PersistenceConfig` that connects to the dockerized Hazelcast cluster as a Hazelcast client.

### Package Structure

- **`config/`** - `@Configuration` beans:
  - `PersistenceConfig` - wires two `JpaSession` beans (`skyBlockSession` via
    `RemoteSkyBlockFactory` + `JpaCacheProvider.HAZELCAST_CLIENT`, and `assetSession` via
    `JpaCacheProvider.EHCACHE`), plus the Phase 6b `skyBlockWriteHazelcastInstance` bean
    (a second Hazelcast client alongside the JCache-managed instance used for direct
    IQueue / IMap access on the write path). Registers the Phase 6b `remoteSkyBlockFactory`
    bean that the write-path beans inject to iterate the `getWritableSources()` registry.
  - `GitHubConfig` - wires three Feign clients against `api.github.com`: the read-path
    `skyBlockDataClient` (`Accept: application/vnd.github.raw+json` for raw file bodies),
    the Phase 6b write-path `skyBlockDataWriteClient` (`Accept: application/vnd.github+json`
    for JSON envelopes + PUT), and the Phase 6b dormant `skyBlockGitDataClient` (same JSON
    media type, surface only - no production callers). All three share the same
    `skyBlockDataAuthorizationSupplier` so one PAT covers every path.

- **`client/`** - Feign contract interfaces + DTOs:
  - `SkyBlockDataContract` - read path (Contents API raw media type).
  - `SkyBlockDataWriteContract` - Phase 6b write path (Contents API JSON media type + PUT).
  - `SkyBlockGitDataContract` - Phase 6b dormant Git Data API surface (getRef, getCommit,
    getTree, createBlob, createTree, createCommit, updateRef) shipped with DTO round-trip
    tests but no production caller. Reserved for a future Phase 6e multi-file commit
    coalescing path.

- **`persistence/`** - `RemoteSkyBlockFactory` (wraps each model's source chain as
  `WritableRemoteJsonSource(DiskOverlaySource(RemoteJsonSource(...)))` and exposes
  `getWritableSources()` for the write-path beans) + `WritableRemoteJsonSource` (per-entity
  buffer, `commitBatch` with manifest path resolution, 412 retry with blob SHA refetch,
  escalation of failed mutations to the caller's retry queue).

- **`poller/`** - Phase 4c `AssetPoller` watchdog + `AssetDiffEngine` + Phase 5.5
  `RefreshTrigger` SAM for targeted refresh.

- **`write/`** - Phase 6b / 6b.1 write path:
  - `WriteMode` - enum `{GIT_DATA, CONTENTS}` controlling the `WriteBatchScheduler.tick()`
    dispatch. Phase 6b.1 default is `GIT_DATA`; `CONTENTS` is the Phase 6b per-file
    commit path retained as operational fallback.
  - `BufferedMutation<T>` - in-memory record of a single pending mutation (operation,
    hydrated entity, producer request id, buffered timestamp, attempt counter).
    The `attempt` field tracks retry cycles so the scheduler can compute
    `nextAttempt = current + 1` on escalation, producing a bounded retry chain.
  - `RetryEnvelope` - Serializable envelope stored in the `skyblock.writes.retry`
    Hazelcast IMap during exponential backoff. Carries the `WriteRequest` plus
    attempt counter plus absolute `readyAtEpochMillis`. Durable across consumer
    restarts: an in-flight retry scheduled before a crash is picked up by the
    next process's drain loop on its first scan iteration.
  - `StagedBatch<T>` - per-source output of `WritableRemoteJsonSource.stageBatch()`.
    Carries the dirty-file snapshot map (repo-root-relative path → mutated entity list)
    and the list of mutations that drove the staging. Empty batches are silently ignored
    by the scheduler.
  - `BatchCommitRequest` - cross-source aggregation of every non-empty `StagedBatch` in
    a scheduler tick. Merges into a single `Map<String, String>` of file path → new
    UTF-8 body, plus the list of contributing `StagedBatch` instances for dead-letter
    escalation.
  - `GitDataCommitService` - `@Component` orchestrating the 7-step Git Data API flow
    (`getRef` → `getCommit` → `createBlob` × N → `createTree(base_tree)` → `createCommit`
    → `updateRef`). 3 immediate retries on 409/422 non-fast-forward, escalate to backoff
    otherwise. Returns a `GitDataCommitResult` (success with new commit SHA, or failure
    with root cause).
  - `WriteQueueConsumer` - `@Component` daemon thread draining the `skyblock.writes`
    Hazelcast IQueue and the `skyblock.writes.retry` Hazelcast IMap in alternation.
    Each drain iteration polls the IQueue for 500ms; if nothing arrives, the loop
    scans the retry IMap for entries whose `readyAt` has elapsed and atomically
    removes + dispatches each eligible entry. The dispatch path is unified: both
    fresh IQueue requests and retry IMap entries go through a single `dispatch`
    method that resolves the entity class, rehydrates via Gson, and buffers a
    `BufferedMutation` with the supplied attempt counter. Dead-letters exhausted
    retries to the `skyblock.writes.deadletter` IMap for operator inspection.
  - `WriteBatchScheduler` - `@Component` with `@Scheduled(fixedDelayString=10s)` that
    dispatches on `WriteMode`:
    - `GIT_DATA` (default): phase A stages every dirty source, phase B merges into a
      `BatchCommitRequest` and hands it to `GitDataCommitService` for a single atomic
      commit spanning every dirty file. Matches the original Q4 commit message format.
    - `CONTENTS` (fallback): iterates sources and calls `commitBatch()` on each,
      producing one Contents API PUT commit per dirty source per tick.
    Failed mutations escalate to the consumer's retry IMap as `RetryEnvelope`
    entries with `attempt = mutation.getAttempt() + 1` and the configured
    exponential-backoff ready instant computed via
    `RetryEnvelope.computeReadyAt`. The original producer request id is
    preserved byte-identically across escalation cycles via
    `WriteRequest.withRequestId(UUID)` (Phase 6b.2) so dead-letter queries
    keyed on `requestId` correlate end-to-end from the initial producer
    put through every retry attempt.
  - `SmokeWriteSentinel` - `@Profile("smoke")` bean that puts a single synthetic
    `WriteRequest` on startup for gate-7 end-to-end docker testing. Emits a sentinel
    `ZodiacEvent` with id `SBS_WRITE_SMOKE_TEST` and expects the operator to revert
    the mutation after verification.

### Dependencies

- **`minecraft-api`** - reuses the model layer and `SkyBlockFactory` from the existing
  Hypixel/Mojang client module. Provides the parameterized
  `MinecraftApi.connectSkyBlockSession(JpaCacheProvider)` overload that this module's
  `PersistenceConfig` delegates to.
- **`spring-boot-starter`** - context, lifecycle, configuration.
- **`spring-boot-starter-actuator`** - health and metrics endpoints (Phase 2c verification).
- **`com.hazelcast:hazelcast` 5.6.0** (`implementation`) - Hazelcast Java client. Promoted
  from `runtimeOnly` to `implementation` in Phase 6b because `PersistenceConfig`,
  `WriteQueueConsumer`, and `WriteBatchScheduler` now reference direct Hazelcast symbols
  (`HazelcastClient`, `HazelcastInstance`, `IQueue`, `IMap`). Earlier phases only used
  Hazelcast indirectly through the JCache SPI so `runtimeOnly` was sufficient.

### Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SKYBLOCK_DATA_GITHUB_TOKEN` | required (Phase 5+) | empty | Fine-grained PAT used by `GitHubConfig` for the `Authorization: Bearer <token>` header on every GitHub REST API call. No repository access is required - public repo content is always readable. Phase 5 loads all 41 SkyBlock entity JSONs from the skyblock-data repo at startup; without a PAT the 60 req/hr unauthenticated budget is exhausted mid-boot and `JpaSession.cacheRepositories()` throws `JpaException`, causing Spring to fail context refresh and the container to exit non-zero. Set the variable to unlock the 5000 req/hr authenticated budget. |
| `SKYBLOCK_DATA_OVERLAY_PATH` | optional | `skyblock-data-overlay` | Base directory for `DiskOverlaySource` local-override lookups. Resolved as a `java.nio.file.Path`, either relative to the JVM working directory or absolute. The `RemoteSkyBlockFactory` joins this with `<@Table.name>.json` per model. Default resolves to a non-existent directory so production `DiskOverlaySource.load()` calls fall straight through to `RemoteJsonSource`. Dev contributors point this at a local `skyblock-data` checkout to test overrides without pushing to GitHub. |
| `SKYBLOCK_HAZELCAST_DISABLED` | optional | unset | When set to `true`, disables the Spring context-loads integration test in `SimplifiedDataApplicationTests` so CI without a live Hazelcast cluster can still run the rest of the suite. Does NOT affect production behavior. |

Phase 4b reads `SKYBLOCK_DATA_GITHUB_TOKEN` via the Spring property placeholder
`skyblock.data.github.token` defined in `application.properties`. The PAT is never persisted
anywhere; it is resolved at context refresh, wrapped in a `Supplier<Optional<String>>`, and
invoked per outbound HTTP request by the `dev.simplified.client.Client` dynamic-header
interceptor.
