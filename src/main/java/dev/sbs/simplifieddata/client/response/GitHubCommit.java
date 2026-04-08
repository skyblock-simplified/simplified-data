package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-bindable mirror of a single entry in the GitHub "List commits" response.
 *
 * <p>Only the fields consumed by the Phase 4c change-detection pipeline are declared; every
 * other field in the upstream JSON is silently ignored by Gson's reflective binder. The top-level
 * {@link #sha} is the branch-tip commit id the poller compares against
 * {@code ExternalAssetState.commitSha}; {@link CommitDetail#committer} carries the ISO-8601
 * timestamp that the operator runbook surfaces for observability.
 *
 * <p>Instances are produced by {@link com.google.gson.Gson#fromJson} inside the
 * {@link dev.simplified.client.Client} response decoder pipeline - never constructed directly
 * by application code, which is why the constructor is private.
 *
 * @see <a href="https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28">GitHub list commits</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitHubCommit {

    /** The commit SHA at the branch tip. */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** The nested {@code commit} object carrying author and committer metadata. */
    @SerializedName("commit")
    private final @NotNull CommitDetail commit;

    /**
     * Nested {@code commit} object inside the top-level commit response.
     *
     * <p>Narrowed to the two fields Phase 4c cares about - the commit message for log output
     * and the committer actor for the ISO-8601 timestamp.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class CommitDetail {

        /** The commit message body as produced by the author. */
        @SerializedName("message")
        private final @NotNull String message;

        /** The committer actor carrying name, email, and ISO-8601 date. */
        @SerializedName("committer")
        private final @NotNull Actor committer;

    }

    /**
     * A GitHub actor (author or committer) embedded inside {@link CommitDetail}.
     *
     * <p>Fields are kept as plain strings; callers that need a {@code java.time.Instant}
     * can parse {@link #date} lazily via {@code Instant.parse(...)}.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Actor {

        /** The display name of the actor. */
        @SerializedName("name")
        private final @NotNull String name;

        /** The ISO-8601 UTC timestamp at which the commit was authored or committed. */
        @SerializedName("date")
        private final @NotNull String date;

    }

}
