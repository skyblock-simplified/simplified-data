package dev.sbs.simplifieddata.client;

import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.simplified.client.request.Contract;
import dev.simplified.client.route.Route;
import feign.Param;
import feign.RequestLine;
import org.jetbrains.annotations.NotNull;

/**
 * Feign contract for the subset of the GitHub REST API required by the {@code skyblock-data}
 * Phase 4 pipeline - fetching the latest commit on {@code master} and the raw file body of
 * any path under {@code data/v1/}.
 *
 * <p>Authentication is wired by {@link dev.sbs.simplifieddata.config.GitHubConfig} as a
 * dynamic {@code Authorization} header sourced from the {@code SKYBLOCK_DATA_GITHUB_TOKEN}
 * environment variable. A missing token degrades the client to unauthenticated mode (60
 * requests per hour per IP) rather than failing the bean construction - every request still
 * succeeds at the lower rate, which is sufficient for Phase 4c's once-per-minute poller in
 * dev environments.
 *
 * <p>The {@code X-GitHub-Api-Version: 2022-11-28} header is pinned via a static client header
 * in {@code GitHubConfig} rather than inline here, so a future bump to the header value is a
 * one-line config change.
 *
 * <p>The {@link #getFileContent(String)} method uses the
 * {@code application/vnd.github.raw+json} {@code Accept} media type, wired as a static
 * request-scoped header in {@code GitHubConfig}. This is load-bearing: it is the only
 * Contents API media type that returns the raw file body directly for files larger than 1 MB
 * (the {@code items.json} primary asset is 6.78 MB). Without this header the Contents endpoint
 * returns a base64-encoded JSON envelope capped at 1 MB which would reject {@code items.json}.
 *
 * <p>Conditional {@code If-None-Match} requests are handled automatically by the
 * Phase 5.5.1 client-library update: the internal request interceptor auto-attaches the
 * header on every {@code GET} when a matching cached response exists, and the response
 * interceptor transparently serves cached bodies on {@code 304} by synthesizing a response
 * envelope with {@link dev.simplified.client.response.HttpStatus#NOT_MODIFIED}. No caller
 * wiring is required.
 *
 * @see <a href="https://docs.github.com/en/rest?apiVersion=2022-11-28">GitHub REST API v3</a>
 * @see dev.sbs.simplifieddata.config.GitHubConfig
 */
@Route("api.github.com")
public interface SkyBlockDataContract extends Contract {

    /**
     * Fetches the current tip commit of the {@code master} branch of
     * {@code skyblock-simplified/skyblock-data}.
     *
     * <p>Uses the single-commit-by-ref endpoint ({@code /commits/master}) rather than the
     * listing endpoint ({@code /commits?sha=master&per_page=1}). This is load-bearing: the
     * listing endpoint serves responses through GitHub's 60-second edge cache which can
     * return stale commit SHAs for many minutes after a push (the {@code cache-control}
     * header on that endpoint is {@code private, max-age=60, s-maxage=60}). The single-
     * commit-by-ref endpoint resolves {@code master} via the git-protocol ref lookup and
     * is always fresh, matching the freshness guarantees of
     * {@code /git/refs/heads/master}.
     *
     * <p>Callers read {@link GitHubCommit#getSha()} for the branch-tip SHA that Phase 4c's
     * {@code AssetPoller} compares against {@code ExternalAssetState.commitSha}. The
     * response {@code ETag} header is available via
     * {@link dev.simplified.client.Client#getLastResponse()} and drives the automatic
     * {@code If-None-Match} path on subsequent polls.
     *
     * @return the current tip commit on master
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/commits/master")
    @NotNull GitHubCommit getLatestMasterCommit() throws SkyBlockDataException;

    /**
     * Fetches the raw file body at the given path on the {@code master} branch of
     * {@code skyblock-simplified/skyblock-data}.
     *
     * <p>Returns the literal file bytes, not a JSON envelope with base64 content. This path
     * works for every file in the {@code skyblock-data} repo (the largest is
     * {@code data/v1/items/items.json} at ~6.78 MB, comfortably below the GitHub-documented
     * 100 MB ceiling for the Contents API raw media type).
     *
     * <p>The return type is {@code byte[]} (not {@code String}) because the framework's
     * {@code InternalResponseDecoder} attempts to parse the raw body as JSON when the target
     * type is {@code String}, producing {@code null} for JSON-object bodies and crashing at
     * {@code Response$Impl.<init>} with a {@code body is marked non-null but is null} error.
     * Routing through the binary-body decoder path avoids this entirely; callers decode the
     * bytes to a UTF-8 string via {@link java.nio.charset.StandardCharsets#UTF_8} in one line.
     *
     * @param path the repo-root-relative file path from a
     *             {@link dev.simplified.persistence.source.ManifestIndex.Entry},
     *             e.g. {@code "data/v1/items/items.json"}
     * @return the raw file body bytes
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/contents/{path}?ref=master")
    byte @NotNull [] getFileContent(@Param("path") @NotNull String path) throws SkyBlockDataException;

}
