package dev.sbs.simplifieddata.config;

import com.google.gson.Gson;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.client.SkyBlockGitDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.source.GitHubFileFetcher;
import dev.sbs.simplifieddata.source.GitHubIndexProvider;
import dev.simplified.client.Client;
import dev.simplified.client.ClientConfig;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Wires the GitHub-backed {@code skyblock-data} client and Phase 4a source bridge beans into
 * the {@code simplified-data} Spring context.
 *
 * <p>The PAT is read from the {@code SKYBLOCK_DATA_GITHUB_TOKEN} environment variable via the
 * Spring property placeholder {@code skyblock.data.github.token}. A missing or blank value
 * degrades the client to unauthenticated mode (60 req/hr per IP) and logs a warning rather
 * than failing the context refresh - public repo reads still succeed, which lets dev
 * environments boot without provisioning a token. Production deployments should always set
 * the variable for the 5000 req/hr authenticated budget.
 *
 * <p>Bean graph:
 * <ul>
 *   <li>{@code skyBlockDataAuthorizationSupplier} - Spring-injected supplier of the
 *       {@code Authorization} header value, resolved once per outbound request via the
 *       {@link dev.simplified.client.Client} dynamic-header pipeline.</li>
 *   <li>{@code skyBlockDataClient} - the {@link Client} wrapping the
 *       {@link SkyBlockDataContract} proxy.</li>
 *   <li>{@code gitHubIndexProvider} - the Phase 4a {@link IndexProvider} bridge.</li>
 *   <li>{@code gitHubFileFetcher} - the Phase 4a {@link FileFetcher} bridge.</li>
 * </ul>
 *
 * <p>Phase 5.5.1 note - {@code If-None-Match} conditional requests are handled automatically
 * by the {@code dev.simplified.client} library. {@code InternalRequestInterceptor} attaches
 * the header on every {@code GET} / {@code HEAD} when a matching cached response exists in
 * {@link Client#getResponseCache()}, and {@code InternalResponseInterceptor} transparently
 * serves the cached body on {@code 304} by synthesizing a fresh response envelope with
 * {@link dev.simplified.client.response.HttpStatus#NOT_MODIFIED} as the wire-truth status.
 * Callers only see a {@link dev.simplified.client.exception.NotModifiedException} on the
 * rare cache-miss revalidation path (client restart, TTL prune, streaming endpoint). No
 * explicit {@code If-None-Match} supplier is needed here - the framework does it.
 *
 * <p>None of these beans issue any network I/O at construction. The {@link Client} wrapper
 * builds the Feign proxy and Apache HttpClient connection pool lazily on the first contract
 * method invocation, so an unreachable {@code api.github.com} at boot does NOT prevent
 * simplified-data from starting.
 */
@Configuration
@Log4j2
public class GitHubConfig {

    /** The human-readable source id used in exception messages and Phase 4c's asset state. */
    public static final @NotNull String SOURCE_ID = "skyblock-data";

    /** The GitHub REST API version header value locked by the research pack. */
    private static final @NotNull String GITHUB_API_VERSION = "2022-11-28";

    /** The {@code Accept} media type required by the Contents endpoint for files above 1 MB. */
    private static final @NotNull String GITHUB_RAW_ACCEPT = "application/vnd.github.raw+json";

    /**
     * The default GitHub JSON media type used by the Phase 6b write-path client. Returns the
     * JSON envelope from {@code GET /contents/{path}} (with the blob {@code sha} field needed
     * as the optimistic-concurrency token for {@code PUT}) rather than the raw file body.
     */
    private static final @NotNull String GITHUB_JSON_ACCEPT = "application/vnd.github+json";

    /**
     * Builds the dynamic supplier of the {@code Authorization} header value from the
     * Spring-resolved PAT property.
     *
     * <p>A blank or missing token returns {@link Optional#empty()} on every invocation so the
     * framework's request interceptor omits the header entirely, leaving the client in
     * unauthenticated mode. A non-blank token returns {@code Bearer <token>} verbatim.
     *
     * @param token the PAT resolved from {@code skyblock.data.github.token} / the
     *              {@code SKYBLOCK_DATA_GITHUB_TOKEN} env var; may be empty
     * @return the dynamic-header supplier
     */
    @Bean
    public @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier(
        @Value("${skyblock.data.github.token:}") @NotNull String token
    ) {
        if (token.isBlank()) {
            log.warn(
                "SKYBLOCK_DATA_GITHUB_TOKEN is not set - falling back to unauthenticated GitHub "
                    + "access (60 req/hr per IP). Set the environment variable for the 5000 req/hr "
                    + "authenticated budget."
            );
            return Optional::empty;
        }

        String headerValue = "Bearer " + token;
        log.info(
            "SKYBLOCK_DATA_GITHUB_TOKEN loaded (length={}) - GitHub client will use "
                + "authenticated requests (5000 req/hr budget)",
            token.length()
        );
        return () -> Optional.of(headerValue);
    }

    /**
     * Builds the {@link Client} for the {@link SkyBlockDataContract}.
     *
     * <p>Wires three headers:
     * <ul>
     *   <li>{@code Accept} (static) - pinned to {@code application/vnd.github.raw+json} so the
     *       Contents endpoint returns raw file bodies for files above 1 MB. The Commits
     *       endpoint accepts this media type as a no-op alias for its standard JSON.</li>
     *   <li>{@code X-GitHub-Api-Version} (static) - pinned to {@code 2022-11-28}.</li>
     *   <li>{@code Authorization} (dynamic) - sourced from
     *       {@code skyBlockDataAuthorizationSupplier}.</li>
     * </ul>
     *
     * <p>{@code If-None-Match} is handled automatically by the
     * {@code dev.simplified.client.interceptor.InternalRequestInterceptor} auto-attach path
     * introduced in the Phase 5.5.1 client-library update - see the class-level Javadoc.
     * No explicit header wiring is required here.
     *
     * <p>The error decoder maps every non-2xx response to a {@link SkyBlockDataException},
     * matching the {@code HypixelApiException} / {@code SbsApiException} /
     * {@code MojangApiException} pattern in {@code minecraft-api}.
     *
     * <p>The Gson instance is obtained from {@link DataApi#getGson()} so the client
     * reuses the {@code ConcurrentList} type adapter, the {@code JpaExclusionStrategy}, and
     * the SkyBlock type adapters. Creating a fresh {@code new Gson()} here would break
     * {@code ConcurrentList<GitHubCommit>} deserialization on the commit response.
     *
     * @param skyBlockDataAuthorizationSupplier the dynamic {@code Authorization} supplier
     * @return the fully constructed client wrapper; no network I/O performed during build
     */
    @Bean
    public @NotNull Client<SkyBlockDataContract> skyBlockDataClient(
        @Qualifier("skyBlockDataAuthorizationSupplier") @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier
    ) {
        Gson gson = DataApi.getGson();

        ClientConfig<SkyBlockDataContract> options = ClientConfig.builder(SkyBlockDataContract.class, gson)
            .withHeader("Accept", GITHUB_RAW_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", skyBlockDataAuthorizationSupplier)
            .withErrorDecoder((methodKey, response) -> {
                throw new SkyBlockDataException(methodKey, response);
            })
            .build();

        log.info("Building SkyBlockDataContract client against api.github.com");
        return Client.create(options);
    }

    /**
     * Builds the Phase 6b write-path {@link Client} for
     * {@link SkyBlockDataWriteContract}.
     *
     * <p>Structurally identical to {@link #skyBlockDataClient} except for the static
     * {@code Accept} header: the write-path client requests
     * {@code application/vnd.github+json} so the Contents API {@code GET} endpoint
     * returns the JSON envelope (including the blob {@code sha} field) and the
     * {@code PUT} endpoint accepts a standard JSON body. Merging the write path
     * onto the existing read-path client would attach two {@code Accept} headers
     * to every request ({@code Client.buildInternalClient()} adds static client
     * headers via {@code request.addHeader(...)}, which is additive rather than
     * replacing), producing brittle content-negotiation behavior.
     *
     * <p>Shares the same {@code skyBlockDataAuthorizationSupplier} bean with the
     * read-path client so a single PAT resolves both contracts' auth. The
     * {@code If-None-Match} auto-attach pipeline introduced in Phase 5.5.1 is
     * inherited transparently - the framework's
     * {@code InternalRequestInterceptor} auto-attaches the header on every
     * {@code GET} on this client as well.
     *
     * <p>{@code If-None-Match} is not auto-attached on the write client's
     * {@code PUT} calls because
     * {@code InternalRequestInterceptor.attachIfNoneMatch} restricts itself to
     * {@code GET}/{@code HEAD} methods - mutating calls never get surprise
     * optimistic-concurrency enforcement via the auto-attach path. The Phase 6b
     * retry path manages its own blob SHA explicitly through the
     * {@link PutContentRequest#getSha()} field of the body.
     *
     * @param skyBlockDataAuthorizationSupplier the dynamic {@code Authorization} supplier,
     *                                          shared with the read-path client
     * @return the write-path client wrapper
     */
    @Bean
    public @NotNull Client<SkyBlockDataWriteContract> skyBlockDataWriteClient(
        @Qualifier("skyBlockDataAuthorizationSupplier") @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier
    ) {
        Gson gson = DataApi.getGson();

        ClientConfig<SkyBlockDataWriteContract> options = ClientConfig.builder(SkyBlockDataWriteContract.class, gson)
            .withHeader("Accept", GITHUB_JSON_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", skyBlockDataAuthorizationSupplier)
            .withErrorDecoder((methodKey, response) -> {
                throw new SkyBlockDataException(methodKey, response);
            })
            .build();

        log.info("Building SkyBlockDataWriteContract client against api.github.com (Accept: {})", GITHUB_JSON_ACCEPT);
        return Client.create(options);
    }

    /**
     * Unwraps the Phase 6b write-path client's contract proxy as a standalone bean so
     * that {@code WritableRemoteJsonSource} and the Phase 6b write-path beans do not
     * need to depend on the {@code final} {@link Client} type directly. Matches the
     * {@code skyBlockDataContract} pattern on {@link dev.sbs.simplifieddata.config.PersistenceConfig}.
     *
     * @param skyBlockDataWriteClient the write-path client wrapper
     * @return the unwrapped {@link SkyBlockDataWriteContract} proxy
     */
    @Bean
    public @NotNull SkyBlockDataWriteContract skyBlockDataWriteContract(
        @NotNull Client<SkyBlockDataWriteContract> skyBlockDataWriteClient
    ) {
        return skyBlockDataWriteClient.getContract();
    }

    /**
     * Builds the dormant Phase 6b {@link Client} for
     * {@link SkyBlockGitDataContract} - the extractable Git Data API surface.
     *
     * <p>Structurally identical to {@link #skyBlockDataWriteClient} (same
     * {@code Accept: application/vnd.github+json}, same API version header,
     * same auth supplier) because the Git Data API accepts the same JSON
     * media type and the same PAT. No production code path reads or writes
     * through this client in Phase 6b - it is shipped purely as an
     * extractable API surface for future initiatives (Phase 6e multi-file
     * commit coalescing; downstream project extraction).
     *
     * <p>The bean is eagerly constructed at context refresh so the Feign
     * proxy validation runs at boot: any broken {@code @RequestLine}
     * template on {@link SkyBlockGitDataContract} fails fast rather than
     * surfacing at first use. Network I/O still does not happen until a
     * contract method is invoked.
     *
     * @param skyBlockDataAuthorizationSupplier the dynamic {@code Authorization} supplier,
     *                                          shared with both other GitHub clients
     * @return the Git Data API client wrapper
     */
    @Bean
    public @NotNull Client<SkyBlockGitDataContract> skyBlockGitDataClient(
        @Qualifier("skyBlockDataAuthorizationSupplier") @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier
    ) {
        Gson gson = DataApi.getGson();

        ClientConfig<SkyBlockGitDataContract> options = ClientConfig.builder(SkyBlockGitDataContract.class, gson)
            .withHeader("Accept", GITHUB_JSON_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", skyBlockDataAuthorizationSupplier)
            .withErrorDecoder((methodKey, response) -> {
                throw new SkyBlockDataException(methodKey, response);
            })
            .build();

        log.info("Building SkyBlockGitDataContract client against api.github.com (dormant write surface)");
        return Client.create(options);
    }

    /**
     * Unwraps the dormant Git Data client's contract proxy as a standalone
     * bean. Matches the {@code skyBlockDataContract} / {@code skyBlockDataWriteContract}
     * pattern.
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
     * @param skyBlockDataClient the GitHub client wrapper
     * @return a Phase 4a {@link IndexProvider} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull IndexProvider gitHubIndexProvider(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return new GitHubIndexProvider(SOURCE_ID, skyBlockDataClient.getContract(), DataApi.getGson());
    }

    /**
     * Registers the {@link GitHubFileFetcher} bridge bean.
     *
     * @param skyBlockDataClient the GitHub client wrapper
     * @return a Phase 4a {@link FileFetcher} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull FileFetcher gitHubFileFetcher(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return new GitHubFileFetcher(SOURCE_ID, skyBlockDataClient.getContract());
    }

}
