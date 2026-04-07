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
| 2c | current | Spring Boot scaffold + Hazelcast client wiring + L2 cache validation |
| 3 | future | Move 46 JSONs into the external skyblock-data repo |
| 4 | future | RemoteJsonSource + DiskOverlaySource + asset polling pipeline |
| 5 | future | Switch to RemoteJsonSource backed by skyblock-data |
| 6 | future | IQueue write consumer (skyblock.writes) |

### Entry Point

- **`SimplifiedData`** - Spring Boot application. Headless (no web server). Bootstraps a `JpaSession`
  via `PersistenceConfig` that connects to the dockerized Hazelcast cluster as a Hazelcast client.

### Package Structure

- **`config/`** - `@Configuration` beans:
  - `PersistenceConfig` - one-liner bean delegating to `MinecraftApi.connectSkyBlockSession(JpaCacheProvider.HAZELCAST_CLIENT)`,
    which preserves every locked-correct setting (H2 in-memory driver, schema `"skyblock"`, the
    registered `SkyBlockFactory`, `GsonSettings.StringType.DEFAULT` mutation, query cache, second-level
    cache, `READ_WRITE` concurrency, `CREATE_WARN` missing-cache strategy, 30-second query TTL) and
    varies only the cache provider. The Hazelcast client config is loaded from
    `src/main/resources/hazelcast-client.xml` (which must stay in sync with
    `infra/hazelcast/hazelcast-client.xml`).

### Dependencies

- **`minecraft-api`** - reuses the model layer and `SkyBlockFactory` from the existing
  Hypixel/Mojang client module. Provides the parameterized
  `MinecraftApi.connectSkyBlockSession(JpaCacheProvider)` overload that this module's
  `PersistenceConfig` delegates to.
- **`spring-boot-starter`** - context, lifecycle, configuration.
- **`spring-boot-starter-actuator`** - health and metrics endpoints (Phase 2c verification).
- **`com.hazelcast:hazelcast` 5.6.0** (`runtimeOnly`) - Hazelcast Java client. The persistence
  library declares this as `compileOnly`, so each consumer that opts into a `HAZELCAST_*`
  provider must add the runtime dep itself. The matching `testRuntimeOnly` is required by
  `JpaModelHazelcastTest` because Gradle's `runtimeOnly` does not cascade into
  `testRuntimeClasspath`.
