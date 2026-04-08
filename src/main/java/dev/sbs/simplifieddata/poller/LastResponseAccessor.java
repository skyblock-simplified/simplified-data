package dev.sbs.simplifieddata.poller;

import dev.simplified.client.response.Response;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * Functional interface supplying the most recent {@code Client.getLastResponse()} value
 * to {@link AssetPoller}.
 *
 * <p>The Phase 4b {@link dev.simplified.client.Client} class is {@code final} with a
 * {@code private} constructor, so it cannot be subclassed for tests. Rather than coupling
 * {@link AssetPoller} to the framework implementation type, the poller injects this
 * single-method bridge: production wires it as {@code skyBlockDataClient::getLastResponse},
 * tests supply a hand-rolled lambda returning a synthetic {@link Response}.
 *
 * <p>This indirection costs zero runtime overhead - method references are inlined by the
 * JIT - and keeps {@link AssetPoller} and its tests entirely free of the framework's
 * connection-pool, retry, and Feign internals.
 *
 * @see AssetPoller
 * @see dev.simplified.client.Client#getLastResponse()
 */
@FunctionalInterface
public interface LastResponseAccessor {

    /**
     * Returns the most recently cached HTTP response on the underlying client, if any.
     *
     * @return an {@link Optional} carrying the last response, or empty when no call has been made
     */
    @NotNull Optional<Response<?>> getLastResponse();

}
