package dev.sbs.simplifieddata.config;

import api.simplified.github.GitHubAuth;
import api.simplified.github.GitHubContentsContract;
import api.simplified.github.GitHubContentsWriteContract;
import api.simplified.github.exception.GitHubApiException;
import com.google.gson.Gson;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.skyblockdata.contract.SkyBlockDataContract;
import dev.sbs.skyblockdata.contract.SkyBlockDataService;
import dev.sbs.skyblockdata.contract.SkyBlockGitDataContract;
import dev.sbs.skyblockdata.source.GitHubFileFetcher;
import dev.sbs.skyblockdata.source.GitHubIndexProvider;
import dev.simplified.client.Client;
import dev.simplified.client.ClientConfig;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the GitHub-backed {@code skyblock-data} clients and source bridge beans into the
 * {@code simplified-data} Spring context.
 *
 * <p>The PAT is read from the {@code SKYBLOCK_DATA_GITHUB_TOKEN} environment variable via the
 * Spring property placeholder {@code skyblock.data.github.token}. A missing or blank value
 * degrades the clients to unauthenticated mode (60 req/hr per IP) and logs a warning rather
 * than failing the context refresh - public repo reads still succeed, which lets dev
 * environments boot without provisioning a token. Production deployments should always set
 * the variable for the 5000 req/hr authenticated budget.
 *
 * <p>Three Feign clients are built because the surfaces have distinct {@code Accept} headers:
 * <ul>
 *   <li>Read client - {@link GitHubContentsContract} with {@code application/vnd.github.raw+json}
 *       so the Contents endpoint returns raw file bodies for files above 1 MB.</li>
 *   <li>Write client - {@link GitHubContentsWriteContract} with {@code application/vnd.github+json}
 *       so the Contents {@code GET} returns the JSON envelope (with the blob SHA needed as the
 *       optimistic-concurrency token) and the {@code PUT} accepts a JSON body.</li>
 *   <li>Git Data client - {@link SkyBlockGitDataContract} (a SkyBlock-pre-bound façade over
 *       {@link api.simplified.github.GitHubGitDataContract}) with the JSON {@code Accept}
 *       header. The Git Data API surface drives the multi-file batched commit path.</li>
 * </ul>
 *
 * <p>Two SkyBlock-shaped data façades are exposed for evaluation:
 * <ul>
 *   <li>{@link SkyBlockDataContract} - sub-interface façade aggregating the read and write
 *       proxies behind one type. Source adapters depend on this.</li>
 *   <li>{@link SkyBlockDataService} - service-class façade with the same surface as a final
 *       class. Available alongside the interface variant so each call site can be evaluated
 *       against both ergonomics; the unused variant will be deleted after evaluation.</li>
 * </ul>
 *
 * <p>{@code If-None-Match} is handled automatically by the
 * {@code dev.simplified.client.interceptor.InternalRequestInterceptor} auto-attach path.
 *
 * <p>None of these beans issue any network I/O at construction. The {@link Client} wrapper
 * builds the Feign proxy and Apache HttpClient connection pool lazily on the first contract
 * method invocation, so an unreachable {@code api.github.com} at boot does NOT prevent
 * simplified-data from starting.
 */
@Configuration
@Log4j2
public class GitHubConfig {

    /** The human-readable source id used in exception messages and asset state. */
    public static final @NotNull String SOURCE_ID = "skyblock-data";

    /** The GitHub REST API version header value. */
    private static final @NotNull String GITHUB_API_VERSION = "2022-11-28";

    /** The {@code Accept} media type required by the Contents endpoint for files above 1 MB. */
    private static final @NotNull String GITHUB_RAW_ACCEPT = "application/vnd.github.raw+json";

    /**
     * The default GitHub JSON media type used by the write-path and Git Data clients. Returns
     * the JSON envelope from {@code GET /contents/{path}} (with the blob {@code sha} field
     * needed as the optimistic-concurrency token for {@code PUT}) rather than the raw file body.
     */
    private static final @NotNull String GITHUB_JSON_ACCEPT = "application/vnd.github+json";

    /**
     * Builds the {@link GitHubAuth} supplier from the Spring-resolved PAT property. A blank or
     * missing token degrades the clients to unauthenticated mode.
     *
     * @param token the PAT resolved from {@code skyblock.data.github.token} / the
     *              {@code SKYBLOCK_DATA_GITHUB_TOKEN} env var; may be empty
     * @return the dynamic {@code Authorization} header supplier
     */
    @Bean
    public @NotNull GitHubAuth gitHubAuth(
        @Value("${skyblock.data.github.token:}") @NotNull String token
    ) {
        if (token.isBlank()) {
            log.warn(
                "SKYBLOCK_DATA_GITHUB_TOKEN is not set - falling back to unauthenticated GitHub "
                    + "access (60 req/hr per IP). Set the environment variable for the 5000 req/hr "
                    + "authenticated budget."
            );
        } else {
            log.info(
                "SKYBLOCK_DATA_GITHUB_TOKEN loaded (length={}) - GitHub clients will use "
                    + "authenticated requests (5000 req/hr budget)",
                token.length()
            );
        }
        return GitHubAuth.bearer(token);
    }

    /**
     * Builds the read-side {@link Client} for {@link GitHubContentsContract}. Pinned to the
     * raw {@code Accept} media type so the Contents endpoint returns raw file bodies.
     *
     * @param gitHubAuth the dynamic {@code Authorization} supplier
     * @return the read-path client wrapper
     */
    @Bean
    public @NotNull Client<GitHubContentsContract> gitHubContentsClient(@NotNull GitHubAuth gitHubAuth) {
        Gson gson = DataApi.getGson();
        ClientConfig<GitHubContentsContract> options = ClientConfig.builder(GitHubContentsContract.class, gson)
            .withHeader("Accept", GITHUB_RAW_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", gitHubAuth)
            .withErrorDecoder(GitHubApiException::new)
            .build();

        log.info("Building GitHubContentsContract client against api.github.com (Accept: {})", GITHUB_RAW_ACCEPT);
        return Client.create(options);
    }

    /**
     * Builds the write-side {@link Client} for {@link GitHubContentsWriteContract}. Pinned to
     * the JSON {@code Accept} media type so the Contents endpoint returns the JSON envelope
     * with the blob SHA used as the optimistic-concurrency token.
     *
     * @param gitHubAuth the dynamic {@code Authorization} supplier, shared with the read client
     * @return the write-path client wrapper
     */
    @Bean
    public @NotNull Client<GitHubContentsWriteContract> gitHubContentsWriteClient(@NotNull GitHubAuth gitHubAuth) {
        Gson gson = DataApi.getGson();
        ClientConfig<GitHubContentsWriteContract> options = ClientConfig.builder(GitHubContentsWriteContract.class, gson)
            .withHeader("Accept", GITHUB_JSON_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", gitHubAuth)
            .withErrorDecoder(GitHubApiException::new)
            .build();

        log.info("Building GitHubContentsWriteContract client against api.github.com (Accept: {})", GITHUB_JSON_ACCEPT);
        return Client.create(options);
    }

    /**
     * Aggregates the read and write proxies into the {@link SkyBlockDataContract} façade. This
     * is variant A of the SkyBlock-shaped surface: a Feign sub-interface with default methods
     * pre-binding owner/repo. Source adapters depend on this bean.
     *
     * @param gitHubContentsClient the read-side proxy
     * @param gitHubContentsWriteClient the write-side proxy
     * @return the aggregated {@code SkyBlockDataContract} instance
     */
    @Bean
    public @NotNull SkyBlockDataContract skyBlockDataContract(
        @NotNull Client<GitHubContentsContract> gitHubContentsClient,
        @NotNull Client<GitHubContentsWriteContract> gitHubContentsWriteClient
    ) {
        return SkyBlockDataContract.from(
            gitHubContentsClient.getContract(),
            gitHubContentsWriteClient.getContract()
        );
    }

    /**
     * Wires variant B of the SkyBlock-shaped surface for evaluation against variant A. Both
     * variants delegate to the same underlying read and write proxies; pick whichever
     * ergonomics fit better at the call site after evaluation.
     *
     * @param gitHubContentsClient the read-side proxy
     * @param gitHubContentsWriteClient the write-side proxy
     * @return the service-class façade
     */
    @Bean
    public @NotNull SkyBlockDataService skyBlockDataService(
        @NotNull Client<GitHubContentsContract> gitHubContentsClient,
        @NotNull Client<GitHubContentsWriteContract> gitHubContentsWriteClient
    ) {
        return new SkyBlockDataService(
            gitHubContentsClient.getContract(),
            gitHubContentsWriteClient.getContract()
        );
    }

    /**
     * Builds the {@link Client} for {@link SkyBlockGitDataContract} - the SkyBlock-pre-bound
     * Git Data API façade. Single Feign client; the Git Data API uses one {@code Accept} so
     * no aggregation is required.
     *
     * @param gitHubAuth the dynamic {@code Authorization} supplier, shared with the other clients
     * @return the Git Data API client wrapper
     */
    @Bean
    public @NotNull Client<SkyBlockGitDataContract> skyBlockGitDataClient(@NotNull GitHubAuth gitHubAuth) {
        Gson gson = DataApi.getGson();
        ClientConfig<SkyBlockGitDataContract> options = ClientConfig.builder(SkyBlockGitDataContract.class, gson)
            .withHeader("Accept", GITHUB_JSON_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", gitHubAuth)
            .withErrorDecoder(GitHubApiException::new)
            .build();

        log.info("Building SkyBlockGitDataContract client against api.github.com (Accept: {})", GITHUB_JSON_ACCEPT);
        return Client.create(options);
    }

    /**
     * Unwraps the Git Data client's contract proxy as a standalone bean for direct injection
     * into the write-path orchestrator.
     *
     * @param skyBlockGitDataClient the Git Data API client wrapper
     * @return the unwrapped {@link SkyBlockGitDataContract} proxy
     */
    @Bean
    public @NotNull SkyBlockGitDataContract skyBlockGitDataContract(
        @NotNull Client<SkyBlockGitDataContract> skyBlockGitDataClient
    ) {
        return skyBlockGitDataClient.getContract();
    }

    /**
     * Registers the {@link GitHubIndexProvider} bridge bean.
     *
     * @param skyBlockDataContract the SkyBlock-pre-bound data contract
     * @return an {@link IndexProvider} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull IndexProvider gitHubIndexProvider(@NotNull SkyBlockDataContract skyBlockDataContract) {
        return new GitHubIndexProvider(SOURCE_ID, skyBlockDataContract, DataApi.getGson());
    }

    /**
     * Registers the {@link GitHubFileFetcher} bridge bean.
     *
     * @param skyBlockDataContract the SkyBlock-pre-bound data contract
     * @return a {@link FileFetcher} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull FileFetcher gitHubFileFetcher(@NotNull SkyBlockDataContract skyBlockDataContract) {
        return new GitHubFileFetcher(SOURCE_ID, skyBlockDataContract);
    }

}
