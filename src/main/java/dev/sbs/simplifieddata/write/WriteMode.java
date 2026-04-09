package dev.sbs.simplifieddata.write;

/**
 * Selects which GitHub write path the Phase 6b.1 {@link WriteBatchScheduler}
 * uses on each tick.
 *
 * <p>Controlled by the Spring property
 * {@code skyblock.data.github.write-mode}; default is {@link #GIT_DATA}. Operators
 * can flip to {@link #CONTENTS} if the Git Data API path becomes unhealthy in a
 * way the per-file Contents API does not (e.g. sustained 422 non-fast-forward
 * rejections during high-write periods that never drain).
 */
public enum WriteMode {

    /**
     * Multi-file single-commit via the {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract
     * Git Data API} 7-step flow (ref &rarr; commit &rarr; blob* &rarr; tree &rarr; commit &rarr;
     * ref update). Every file mutated inside a single {@link WriteBatchScheduler} tick lands on
     * {@code skyblock-data/master} in one atomic commit. This is the Phase 6b.1 production
     * default.
     *
     * <p>Side benefit for the {@code items.json} / {@code items_extra.json} split: a single
     * commit can carry updates to both files without ever producing a transient "primary
     * changed but extras stale" state that the
     * {@link dev.sbs.simplifieddata.poller.AssetPoller} would otherwise observe between two
     * Contents-mode commits in the same tick.
     */
    GIT_DATA,

    /**
     * Per-file commits via the {@link dev.sbs.simplifieddata.client.SkyBlockDataWriteContract
     * Contents API}. Every dirty {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource}
     * in a tick produces one commit. Retained as an operational fallback in case the Git Data
     * API path becomes unhealthy or a regression in the 7-step flow blocks writes. This is the
     * Phase 6b default behavior and remains fully tested.
     */
    CONTENTS

}
