package dev.sbs.simplifieddata.poller;

import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Test-friendly SAM bridge between {@link AssetPoller} and
 * {@link JpaSession#refreshModels(Collection)}.
 *
 * <p>Production wires this bean as a method reference to the SkyBlock session's
 * {@code refreshModels} method, so calling {@link #refresh(Collection)} drives the
 * library's 3-phase refresh (source reload, stale removal, L2 eviction) for the
 * specified model subset.
 *
 * <p>Tests supply a hand-rolled lambda that records invocations without needing a
 * live {@link JpaSession}. The {@code final} modifier on {@code JpaSession} prevents
 * subclassing, and mocking it with Mockito would still require a framework dep this
 * module deliberately avoids - so the SAM mirrors the existing
 * {@link LastResponseAccessor} pattern that the Phase 4c poller already uses for the
 * same reason.
 *
 * @see AssetPoller
 * @see LastResponseAccessor
 */
@FunctionalInterface
public interface RefreshTrigger {

    /**
     * Triggers a targeted refresh of the specified model classes on the underlying
     * {@link JpaSession}.
     *
     * @param models the model classes to refresh; implementations must tolerate an
     *               empty collection as a no-op
     */
    void refresh(@NotNull Collection<Class<? extends JpaModel>> models);

}
