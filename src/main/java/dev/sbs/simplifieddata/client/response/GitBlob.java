package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Gson-bindable mirror of a git blob object returned by the GitHub Git Data API.
 *
 * <p>Returned by {@code POST /repos/{owner}/{repo}/git/blobs}. A blob is a
 * raw-content git object that sits underneath a tree - file bytes with no
 * directory metadata. Creating a blob returns only the SHA and URL; the
 * content fields are populated only on the matching
 * {@code GET /git/blobs/{sha}} endpoint (which Phase 6e would not need since
 * the Contents API already covers file reads).
 *
 * <p>Phase 6b ships this DTO as part of the dormant
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract} surface - no
 * production code reads or writes it yet. The Phase 6e Git Data API write
 * path would call {@code createBlob} for each affected file, collect the
 * returned SHAs, and pass them to a follow-up {@code createTree} call.
 *
 * @see <a href="https://docs.github.com/en/rest/git/blobs?apiVersion=2022-11-28">GitHub Git blobs</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitBlob {

    /** The blob SHA. */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** The GitHub API URL for the blob. */
    @SerializedName("url")
    private final @NotNull String url;

    /**
     * The blob size in bytes. Populated only on {@code GET} responses; null
     * on the {@code POST /blobs} create response.
     */
    @SerializedName("size")
    private final @Nullable Long size;

    /**
     * The blob content. Populated only on {@code GET} responses with encoding
     * {@code "base64"} or {@code "utf-8"}; null on the {@code POST} create
     * response.
     */
    @SerializedName("content")
    private final @Nullable String content;

    /**
     * The encoding marker for {@link #content}. Populated only on {@code GET}
     * responses; null on the {@code POST} create response.
     */
    @SerializedName("encoding")
    private final @Nullable String encoding;

}
