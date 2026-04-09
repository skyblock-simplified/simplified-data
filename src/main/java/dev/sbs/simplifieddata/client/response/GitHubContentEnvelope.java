package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-bindable mirror of the GitHub Contents API JSON envelope returned by
 * {@code GET /repos/{owner}/{repo}/contents/{path}} when the caller requests the
 * default {@code application/vnd.github+json} media type.
 *
 * <p>The critical field for the Phase 6b write path is {@link #sha}, which is the
 * git <b>blob</b> SHA of the file at the branch tip. The write path uses this value
 * as the optimistic-concurrency token on the follow-up
 * {@code PUT /repos/{owner}/{repo}/contents/{path}} call: if another writer committed
 * to the same path between the GET and the PUT, GitHub returns {@code 409} or
 * {@code 422} (historically documented as {@code 409 Conflict}), mapped by the
 * framework to {@code PreconditionFailedException}.
 *
 * <p>Instances are produced by {@link com.google.gson.Gson#fromJson} inside the
 * {@link dev.simplified.client.Client} response decoder pipeline - never constructed
 * directly by application code, which is why the constructor is private under
 * {@link RequiredArgsConstructor}.
 *
 * <p>Only the fields consumed by the Phase 6b pipeline are declared; every other
 * field in the upstream JSON is silently ignored by Gson's reflective binder. The
 * {@link #content} field is base64-encoded (per GitHub's Contents API default
 * encoding) and callers decode it lazily if they need the body - typically
 * Phase 6b does not need it because the write path reads the current file body
 * via the existing read-path {@code SkyBlockDataContract.getFileContent}
 * (which uses {@code Accept: application/vnd.github.raw+json}) to avoid the
 * base64 round-trip.
 *
 * @see <a href="https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-repository-content">
 *      GitHub get repository content</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitHubContentEnvelope {

    /** The file name (without directories). */
    @SerializedName("name")
    private final @NotNull String name;

    /** The full repo-root-relative path of the file. */
    @SerializedName("path")
    private final @NotNull String path;

    /**
     * The blob SHA of the file at the branch tip. Load-bearing: this is the
     * optimistic-concurrency token passed as the {@code sha} field of the follow-up
     * {@code PUT contents} body.
     */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** The file size in bytes, as reported by the envelope. */
    @SerializedName("size")
    private final long size;

    /**
     * The base64-encoded file content, present when the Contents API is invoked with
     * the default {@code application/vnd.github+json} media type. Large files
     * (&gt; 1 MB) return an empty string here per GitHub's envelope limit.
     */
    @SerializedName("content")
    private final @NotNull String content;

    /**
     * The encoding marker for {@link #content}, typically {@code "base64"}. Declared
     * for completeness; callers rarely consult it because the field is always base64
     * when the envelope media type is requested.
     */
    @SerializedName("encoding")
    private final @NotNull String encoding;

}
