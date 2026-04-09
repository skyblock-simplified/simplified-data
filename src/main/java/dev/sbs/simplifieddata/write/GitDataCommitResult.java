package dev.sbs.simplifieddata.write;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Outcome of a single {@link GitDataCommitService#commit(BatchCommitRequest)}
 * invocation.
 *
 * <p>On success, carries the new commit SHA produced by the
 * {@code updateRef} step for the observability log line. On failure, carries
 * the root cause exception for dead-letter inspection.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitDataCommitResult {

    /** The new commit SHA produced by a successful Git Data API flow, or {@code null} on failure. */
    private final @Nullable String commitSha;

    /** The root cause exception on failure, or {@code null} on success. */
    private final @Nullable Throwable failureCause;

    /** @return {@code true} when the commit flow completed all 7 steps successfully. */
    public boolean isSuccess() {
        return this.commitSha != null;
    }

    /** Factory for a success outcome carrying the new commit SHA. */
    public static @NotNull GitDataCommitResult success(@NotNull String commitSha) {
        return new GitDataCommitResult(commitSha, null);
    }

    /** Factory for a failure outcome carrying the root cause. */
    public static @NotNull GitDataCommitResult failure(@NotNull Throwable cause) {
        return new GitDataCommitResult(null, cause);
    }

}
