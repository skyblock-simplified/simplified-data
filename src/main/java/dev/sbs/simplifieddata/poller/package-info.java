/**
 * Phase 4c scheduled watchdog that detects changes in the {@code skyblock-data} GitHub
 * repository and maintains the Phase 4a {@code external_asset_state} /
 * {@code external_asset_entry_state} audit tables.
 *
 * <p>Package layout:
 * <ul>
 *   <li>{@link dev.sbs.simplifieddata.poller.AssetPoller} - Spring {@code @Component} with
 *       {@code @Scheduled} and {@code @EventListener(ApplicationReadyEvent.class)} entry
 *       points. Delegates to a shared {@code doPoll} method that runs the full cycle end to
 *       end.</li>
 *   <li>{@link dev.sbs.simplifieddata.poller.AssetDiffEngine} - pure-function delta engine
 *       comparing a manifest snapshot against persisted per-entry state rows.</li>
 *   <li>{@link dev.sbs.simplifieddata.poller.AssetDiff} - immutable result type with three
 *       lists (added, changed, removed) and an {@code empty} sentinel.</li>
 *   <li>{@link dev.sbs.simplifieddata.poller.LastResponseAccessor} - functional interface
 *       supplying the most recent {@code Client.getLastResponse()} value, decoupling the
 *       poller from the {@code final dev.simplified.client.Client} type for testability.</li>
 * </ul>
 *
 * <p>Phase 4c is deliberately watchdog-only - the poller writes audit state and logs
 * detected changes but never touches any SkyBlock entity repository. Phase 5 wires the
 * change signal to {@code RemoteJsonSource} to make it actionable.
 */
package dev.sbs.simplifieddata.poller;
