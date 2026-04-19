package dev.sbs.simplifieddata;

import com.google.gson.Gson;
import dev.simplified.gson.GsonSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Service locator that replaces the former {@code MinecraftApi} static holder for
 * {@code simplified-data}.
 * <p>
 * Owns the {@link Gson} and {@link GsonSettings} used by the GitHub write path, the asset
 * poller, and the write-path schedulers. {@link GsonSettings#defaults()} walks the
 * {@code ServiceLoader} SPI and picks up contributors from every {@code *-api} jar on the
 * classpath automatically, so this locator never needs to register adapters manually.
 * Persistence access flows through {@code dev.sbs.skyblockdata.SkyBlockData} directly - this
 * locator does not own it.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataApi {

    @Getter private static final @NotNull GsonSettings gsonSettings = GsonSettings.defaults();
    @Getter private static final @NotNull Gson gson = gsonSettings.create();

}
