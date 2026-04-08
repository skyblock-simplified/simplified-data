package dev.sbs.simplifieddata.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link ETagContext}.
 *
 * <p>Covers the empty baseline, the scoped {@link ETagContext#callWithEtag} happy path, the
 * ThreadLocal restoration on nested calls, and the explicit {@code set} / {@code clear}
 * helpers used by Phase 4c tests that want to pre-populate the ETag without a scoped wrapper.
 */
class ETagContextTest {

    @AfterEach
    void tearDown() {
        ETagContext.clear();
    }

    @Test
    @DisplayName("current() returns empty when no ETag is set")
    void currentEmptyByDefault() {
        assertThat(ETagContext.current(), is(notNullValue()));
        assertThat(ETagContext.current(), equalTo(Optional.empty()));
    }

    @Test
    @DisplayName("callWithEtag() propagates the ETag to current() inside the body")
    void callWithEtagPropagates() {
        String etag = "W/\"abc123\"";

        String observed = ETagContext.callWithEtag(etag, () -> ETagContext.current().orElse("missing"));

        assertThat(observed, equalTo(etag));
    }

    @Test
    @DisplayName("callWithEtag() clears the ThreadLocal on exit")
    void callWithEtagClearsOnExit() {
        ETagContext.callWithEtag("W/\"abc123\"", () -> "noop");

        assertThat(ETagContext.current(), equalTo(Optional.empty()));
    }

    @Test
    @DisplayName("nested callWithEtag() restores the outer value on inner exit")
    void callWithEtagNested() {
        String outer = "W/\"outer\"";
        String inner = "W/\"inner\"";

        String observedInner = ETagContext.callWithEtag(outer, () ->
            ETagContext.callWithEtag(inner, () -> ETagContext.current().orElseThrow())
        );

        assertThat(observedInner, equalTo(inner));
    }

    @Test
    @DisplayName("nested callWithEtag() restores the outer value after the inner call returns")
    void callWithEtagNestedRestoresOuter() {
        String outer = "W/\"outer\"";
        String inner = "W/\"inner\"";

        String observedAfterInner = ETagContext.callWithEtag(outer, () -> {
            ETagContext.callWithEtag(inner, () -> "inner-body");
            return ETagContext.current().orElseThrow();
        });

        assertThat(observedAfterInner, equalTo(outer));
    }

    @Test
    @DisplayName("set(null) clears the context")
    void setNullClears() {
        ETagContext.set("W/\"abc\"");
        ETagContext.set(null);

        assertThat(ETagContext.current(), equalTo(Optional.empty()));
    }

}
