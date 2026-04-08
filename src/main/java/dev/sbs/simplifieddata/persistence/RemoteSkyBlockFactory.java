package dev.sbs.simplifieddata.persistence;

import dev.sbs.minecraftapi.persistence.SkyBlockFactory;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.source.DiskOverlaySource;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.RemoteJsonSource;
import dev.simplified.persistence.source.Source;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * A {@link RepositoryFactory} that serves SkyBlock models from GitHub via
 * {@link RemoteJsonSource}, with an optional local-disk overlay layer provided by
 * {@link DiskOverlaySource} for dev-mode workflows.
 *
 * <p>Delegates model enumeration to the existing {@link SkyBlockFactory} bean in
 * {@code minecraft-api} so the authoritative 41-entity list lives in one place. The
 * constructor eagerly instantiates one {@link DiskOverlaySource} per model (each wrapping
 * a per-model {@link RemoteJsonSource}) and stores them in a {@code Class -> Source} map
 * keyed on the model class. {@link #getDefaultSource()} returns {@code null} because every
 * model has an explicit per-type registration.
 *
 * <p>The overlay path is resolved per model by joining the configured base directory with
 * {@code <@Table.name>.json}. A model class missing a {@link Table} annotation triggers an
 * {@link IllegalStateException} during construction - the failure is deliberate and loud,
 * matching the {@code JsonSource} semantics in the persistence library.
 *
 * <p>This class does zero network I/O at construction. Every bean is a lazy lambda that is
 * only invoked on {@code JpaRepository.refresh()} calls, which happen inside
 * {@link dev.simplified.persistence.JpaSession#cacheRepositories()} and subsequent scheduled
 * refreshes. The Phase 4b {@code GitHubIndexProvider} and {@code GitHubFileFetcher} beans
 * are already constructed at this point but have not yet fired any request.
 *
 * @see SkyBlockFactory
 * @see RemoteJsonSource
 * @see DiskOverlaySource
 */
@Log4j2
@Getter
public final class RemoteSkyBlockFactory implements RepositoryFactory {

    /** Suffix appended to {@code @Table.name} when resolving the per-model overlay file path. */
    private static final @NotNull String OVERLAY_SUFFIX = ".json";

    /** The human-readable source id shared with {@code GitHubConfig.SOURCE_ID} and the asset-state rows. */
    private final @NotNull String sourceId;

    /** Model list delegated to the {@link SkyBlockFactory} bean (already topologically sorted). */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /** Per-model source registrations, populated once at construction time. */
    private final @NotNull ConcurrentMap<Class<?>, Source<?>> sources;

    /**
     * Constructs the factory by eagerly wiring one {@link DiskOverlaySource} wrapping one
     * {@link RemoteJsonSource} per model class returned by {@code delegate.getModels()}.
     *
     * @param sourceId the human-readable source id
     * @param delegate the {@link SkyBlockFactory} bean that owns the model list
     * @param indexProvider the Phase 4b {@link IndexProvider} bean
     * @param fileFetcher the Phase 4b {@link FileFetcher} bean
     * @param overlayBasePath the base directory under which per-model overlay files are resolved
     */
    public RemoteSkyBlockFactory(
        @NotNull String sourceId,
        @NotNull SkyBlockFactory delegate,
        @NotNull IndexProvider indexProvider,
        @NotNull FileFetcher fileFetcher,
        @NotNull Path overlayBasePath
    ) {
        this.sourceId = sourceId;
        this.models = delegate.getModels();
        this.sources = Concurrent.newMap();

        for (Class<JpaModel> modelClass : this.models)
            this.sources.put(modelClass, buildSourceFor(modelClass, sourceId, indexProvider, fileFetcher, overlayBasePath));

        log.info(
            "RemoteSkyBlockFactory wired {} models from source '{}' with overlayBasePath='{}'",
            this.models.size(), sourceId, overlayBasePath
        );
    }

    @Override
    public @Nullable Source<?> getDefaultSource() {
        return null;
    }

    @Override
    public @NotNull ConcurrentMap<Class<?>, Consumer<?>> getPeeks() {
        return Concurrent.newUnmodifiableMap();
    }

    /**
     * Builds the per-model source chain: {@code DiskOverlaySource(RemoteJsonSource(...), overlayPath, modelClass)}.
     *
     * @param modelClass the entity type
     * @param sourceId the human-readable source id
     * @param indexProvider the manifest index provider
     * @param fileFetcher the raw-file fetcher
     * @param overlayBasePath the base directory for overlay file resolution
     * @param <T> the entity type
     * @return a fully wrapped source ready to be registered in {@link #sources}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T extends JpaModel> @NotNull Source<T> buildSourceFor(
        @NotNull Class modelClass,
        @NotNull String sourceId,
        @NotNull IndexProvider indexProvider,
        @NotNull FileFetcher fileFetcher,
        @NotNull Path overlayBasePath
    ) {
        Table table = (Table) modelClass.getAnnotation(Table.class);

        if (table == null)
            throw new IllegalStateException(
                "Model class '" + modelClass.getName() + "' is missing a @Table annotation - cannot resolve overlay path"
            );

        Path overlayPath = overlayBasePath.resolve(table.name() + OVERLAY_SUFFIX);
        RemoteJsonSource<T> remote = new RemoteJsonSource<>(sourceId, indexProvider, fileFetcher, (Class<T>) modelClass);
        return new DiskOverlaySource<>(remote, overlayPath, (Class<T>) modelClass);
    }

}
