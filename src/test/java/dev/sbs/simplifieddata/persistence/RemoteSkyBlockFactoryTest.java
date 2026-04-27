package dev.sbs.simplifieddata.persistence;

import com.google.gson.Gson;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.skyblockdata.contract.SkyBlockDataContract;
import api.simplified.github.exception.GitHubApiException;
import api.simplified.github.request.PutContentRequest;
import api.simplified.github.response.GitHubContentEnvelope;
import api.simplified.github.response.GitHubPutResponse;
import dev.sbs.simplifieddata.write.WriteMetrics;
import dev.sbs.skyblockdata.SkyBlockFactory;
import dev.sbs.skyblockdata.model.Accessory;
import dev.sbs.skyblockdata.model.Item;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.DiskOverlaySource;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.RemoteJsonSource;
import dev.simplified.persistence.source.Source;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link RemoteSkyBlockFactory}. Does NOT boot a Spring context, does NOT
 * require a live Hazelcast cluster, and does NOT touch the network - the GitHub
 * {@link IndexProvider} and {@link FileFetcher} collaborators are hand-rolled stubs that
 * throw on call. Model enumeration is driven by a fresh {@link SkyBlockFactory} instance
 * constructed directly from {@code skyblock-data-api}.
 */
final class RemoteSkyBlockFactoryTest {

    /** Dummy overlay path - points at a directory that never exists so DiskOverlaySource falls through. */
    private static final @NotNull Path OVERLAY_BASE = Path.of("target/test-overlay-does-not-exist");

    /** Index provider stub that throws if invoked - construction never calls load. */
    private static final @NotNull IndexProvider THROWING_INDEX = () -> {
        throw new JpaException("stub should not be invoked during wiring");
    };

    /** File fetcher stub that throws if invoked - construction never calls fetch. */
    private static final @NotNull FileFetcher THROWING_FETCHER = path -> {
        throw new JpaException("stub should not be invoked during wiring");
    };

    /** Write contract stub that throws on every method - construction never calls GitHub. */
    private static final @NotNull SkyBlockDataContract THROWING_WRITE = new SkyBlockDataContract() {

        @Override
        public api.simplified.github.response.@NotNull GitHubCommit getLatestMasterCommit(@NotNull String owner, @NotNull String repo) throws GitHubApiException {
            throw new IllegalStateException("write contract stub should not be invoked during wiring");
        }

        @Override
        public byte @NotNull [] getFileContent(@NotNull String owner, @NotNull String repo, @NotNull String path) throws GitHubApiException {
            throw new IllegalStateException("write contract stub should not be invoked during wiring");
        }

        @Override
        public @NotNull GitHubContentEnvelope getFileMetadata(@NotNull String owner, @NotNull String repo, @NotNull String path) throws GitHubApiException {
            throw new IllegalStateException("write contract stub should not be invoked during wiring");
        }

        @Override
        public @NotNull GitHubPutResponse putFileContent(@NotNull String owner, @NotNull String repo, @NotNull String path, @NotNull PutContentRequest body) throws GitHubApiException {
            throw new IllegalStateException("write contract stub should not be invoked during wiring");
        }

    };

    private static final @NotNull Gson GSON = DataApi.getGson();

    private static @NotNull RemoteSkyBlockFactory newFactory(@NotNull SkyBlockFactory delegate) {
        return new RemoteSkyBlockFactory(
            "skyblock-data", delegate, THROWING_INDEX, THROWING_FETCHER, THROWING_WRITE, GSON, OVERLAY_BASE, 3,
            new WriteMetrics(new SimpleMeterRegistry())
        );
    }

    @Test
    @SuppressWarnings("rawtypes")
    void wiresEveryModelWithDiskOverlaySourceWrappingRemoteJsonSource() {
        SkyBlockFactory delegate = new dev.sbs.skyblockdata.SkyBlockFactory();
        RemoteSkyBlockFactory factory = newFactory(delegate);

        assertThat(factory.getModels().size(), equalTo(delegate.getModels().size()));
        assertThat(factory.getModels().size(), greaterThanOrEqualTo(41));
        assertThat(factory.getDefaultSource(), nullValue());
        assertThat(factory.getPeeks().isEmpty(), equalTo(true));
        assertThat(factory.getWritableSources().size(), equalTo(delegate.getModels().size()));

        for (Class<JpaModel> modelClass : delegate.getModels()) {
            Source<?> wired = factory.getSources().get(modelClass);
            assertThat("missing source for " + modelClass.getName(), wired, notNullValue());
            assertThat(wired, instanceOf(WritableRemoteJsonSource.class));

            WritableRemoteJsonSource<?> writable = (WritableRemoteJsonSource<?>) wired;
            assertThat(writable.getSourceId(), equalTo("skyblock-data"));
            assertThat(writable.getModelClass(), equalTo((Class) modelClass));
            assertThat(writable.getDelegate(), instanceOf(DiskOverlaySource.class));

            DiskOverlaySource<?> overlay = (DiskOverlaySource<?>) writable.getDelegate();
            assertThat(overlay.getInner(), instanceOf(RemoteJsonSource.class));
            assertThat(overlay.getModelClass(), equalTo((Class) modelClass));

            RemoteJsonSource<?> remote = (RemoteJsonSource<?>) overlay.getInner();
            assertThat(remote.getSourceId(), equalTo("skyblock-data"));
            assertThat(remote.getModelClass(), equalTo((Class) modelClass));

            // The writableSources registry must point at the same instance.
            assertThat(factory.getWritableSources().get(modelClass), equalTo((WritableRemoteJsonSource) writable));
        }
    }

    @Test
    void keyedMapContainsKnownModelClasses() {
        SkyBlockFactory delegate = new dev.sbs.skyblockdata.SkyBlockFactory();
        RemoteSkyBlockFactory factory = newFactory(delegate);

        assertThat(factory.getSources(), hasKey(Item.class));
        assertThat(factory.getSources(), hasKey(Accessory.class));
        assertThat(factory.getWritableSources(), hasKey(Item.class));
        assertThat(factory.getWritableSources(), hasKey(Accessory.class));
    }

    @Test
    void rejectsModelClassWithoutTableAnnotation() {
        SkyBlockFactory delegate = new SkyBlockFactory() {
            @Override
            public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
                ConcurrentList<Class<JpaModel>> single = Concurrent.newList();
                @SuppressWarnings("unchecked")
                Class<JpaModel> unannotated = (Class<JpaModel>) (Class<?>) UnannotatedModel.class;
                single.add(unannotated);
                return single;
            }
        };

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> newFactory(delegate)
        );

        assertThat(ex.getMessage(), allOf(containsString("@Table"), containsString(UnannotatedModel.class.getName())));
    }

    /** Minimal no-@Table JpaModel stand-in for the rejection test. */
    @jakarta.persistence.Entity
    static final class UnannotatedModel implements JpaModel {
    }

}
