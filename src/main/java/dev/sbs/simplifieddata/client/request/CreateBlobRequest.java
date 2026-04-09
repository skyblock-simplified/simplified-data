package dev.sbs.simplifieddata.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Request body for the dormant Git Data API
 * {@code POST /repos/{owner}/{repo}/git/blobs} endpoint on
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract}.
 *
 * <p>A blob is raw file content: no path, no directory metadata, just bytes
 * addressed by the SHA-1 of {@code blob <size>\0<content>}. The Git Data API
 * write path creates one blob per affected file, then stitches them into a
 * tree via a follow-up {@code createTree} call.
 *
 * <p>{@link #encoding} is either {@code "utf-8"} (the content field is sent
 * verbatim UTF-8 text) or {@code "base64"} (the content field is a base64
 * string of the raw bytes). Phase 6e would use {@code "base64"} for all
 * blobs to avoid character-encoding surprises on binary-ish JSON.
 *
 * <p>No production caller in Phase 6b. Shipped with the dormant contract
 * surface.
 *
 * @see <a href="https://docs.github.com/en/rest/git/blobs?apiVersion=2022-11-28#create-a-blob">
 *      GitHub create a blob</a>
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class CreateBlobRequest {

    /** The blob content. Interpret per {@link #encoding}. */
    @SerializedName("content")
    private final @NotNull String content;

    /** The encoding marker: {@code "utf-8"} or {@code "base64"}. */
    @SerializedName("encoding")
    private final @NotNull String encoding;

}
