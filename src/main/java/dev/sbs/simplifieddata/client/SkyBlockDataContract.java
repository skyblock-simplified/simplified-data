package dev.sbs.simplifieddata.client;

import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.simplified.client.request.Contract;
import dev.simplified.client.route.Route;
import dev.simplified.collection.ConcurrentList;
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
 * <p>Phase 4b does not issue conditional {@code If-None-Match} requests. Phase 4c will attach
 * a dynamic {@code If-None-Match} header via {@link ETagContext} and wrap calls with
 * {@link ETagContext#callWithEtag(String, java.util.function.Supplier)}.
 *
 * @see <a href="https://docs.github.com/en/rest?apiVersion=2022-11-28">GitHub REST API v3</a>
 * @see dev.sbs.simplifieddata.config.GitHubConfig
 */
@Route("api.github.com")
public interface SkyBlockDataContract extends Contract {

    /**
     * Fetches the most recent commit on the {@code master} branch of
     * {@code skyblock-simplified/skyblock-data}.
     *
     * <p>Always returns a single-element list ordered newest-first. Callers pull
     * {@code [0].sha} for the branch-tip SHA that Phase 4c will compare against
     * {@code ExternalAssetState.commitSha}. The response ETag is available via
     * {@link dev.simplified.client.Client#getLastResponse()} for conditional requests in
     * Phase 4c.
     *
     * @return single-element list containing the latest commit on master
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/commits?sha=master&per_page=1")
    @NotNull ConcurrentList<GitHubCommit> getLatestMasterCommit() throws SkyBlockDataException;

    /**
     * Fetches the raw file body at the given path on the {@code master} branch of
     * {@code skyblock-simplified/skyblock-data}.
     *
     * <p>The response body is the literal file bytes decoded as UTF-8, not a JSON envelope
     * with base64 content. This path works for every file in the {@code skyblock-data} repo
     * (the largest is {@code data/v1/items/items.json} at ~6.78 MB, comfortably below the
     * GitHub-documented 100 MB ceiling for the Contents API raw media type).
     *
     * @param path the repo-root-relative file path from a
     *             {@link dev.simplified.persistence.source.ManifestIndex.Entry},
     *             e.g. {@code "data/v1/items/items.json"}
     * @return the raw file body as a UTF-8 string
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/contents/{path}?ref=master")
    @NotNull String getFileContent(@Param("path") @NotNull String path) throws SkyBlockDataException;

}
