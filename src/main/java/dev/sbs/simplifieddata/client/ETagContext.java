package dev.sbs.simplifieddata.client;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * ThreadLocal holder that propagates an {@code If-None-Match} ETag value from the caller into
 * the {@link dev.simplified.client.Client} dynamic-header supplier at request time.
 *
 * <p>Phase 4b does not issue conditional requests - the Phase 4b
 * {@link dev.sbs.simplifieddata.source.GitHubIndexProvider} and
 * {@link dev.sbs.simplifieddata.source.GitHubFileFetcher} impls always call through with no
 * ETag set, so the {@code If-None-Match} header is omitted on every outbound request. Phase 4c's
 * scheduled poller will wrap commit lookups via {@link #callWithEtag(String, Supplier)} to opt
 * into the 304 short-circuit path.
 *
 * <p>The ThreadLocal-based indirection is the chosen alternative to Feign's parameterized
 * header template support. The research pack flagged parameterized headers with quoted values
 * (GitHub ETags are emitted as {@code W/"abc123..."} with literal internal double quotes) as
 * unverified on Feign 13.11; the dynamic-header supplier path inside the
 * {@link dev.simplified.client.Client} wrapper is proven working for {@code Authorization},
 * {@code API-Key}, and other headers that carry arbitrary character payloads.
 *
 * <p>Every {@link #callWithEtag(String, Supplier)} invocation restores the previous ThreadLocal
 * value on exit so that nested calls with and without ETags compose cleanly.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ETagContext {

    /**
     * The ThreadLocal holding the currently-propagating ETag, or {@code null} when no
     * conditional request is in flight on this thread.
     */
    private static final @NotNull ThreadLocal<String> CURRENT = new ThreadLocal<>();

    /**
     * Returns the ETag value currently registered for this thread, if any.
     *
     * <p>Called by the dynamic {@code If-None-Match} header supplier registered in
     * {@link dev.sbs.simplifieddata.config.GitHubConfig} on every outbound request.
     *
     * @return the current ETag string, or empty if none is set
     */
    public static @NotNull Optional<String> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    /**
     * Executes the given body with the supplied ETag attached to this thread's context.
     *
     * <p>Restores the previous value on exit (null when no outer call was in flight) so nested
     * calls with and without ETags compose cleanly.
     *
     * @param etag the ETag to register, verbatim including the {@code W/} prefix and the
     *             enclosing double quotes
     * @param body the supplier whose execution should observe the registered ETag
     * @param <R> the supplier return type
     * @return the result of invoking {@code body}
     */
    public static <R> R callWithEtag(@NotNull String etag, @NotNull Supplier<R> body) {
        String previous = CURRENT.get();
        CURRENT.set(etag);

        try {
            return body.get();
        } finally {
            if (previous == null)
                CURRENT.remove();
            else
                CURRENT.set(previous);
        }
    }

    /**
     * Clears any ETag currently registered on this thread.
     *
     * <p>Typically unnecessary - {@link #callWithEtag(String, Supplier)} is the recommended
     * entry point - but exposed for explicit teardown in tests.
     */
    public static void clear() {
        CURRENT.remove();
    }

    /**
     * Manually sets the ETag for this thread without the scoped
     * {@link #callWithEtag(String, Supplier)} wrapper.
     *
     * <p>Intended for tests that assert supplier behavior without an outer call wrapping.
     * Production code should prefer {@link #callWithEtag(String, Supplier)}.
     *
     * @param etag the ETag to register, or {@code null} to clear
     */
    public static void set(@Nullable String etag) {
        if (etag == null)
            CURRENT.remove();
        else
            CURRENT.set(etag);
    }

}
