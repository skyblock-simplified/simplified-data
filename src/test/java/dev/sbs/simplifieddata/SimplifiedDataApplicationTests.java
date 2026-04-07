package dev.sbs.simplifieddata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Spring Boot context-loads test for {@link SimplifiedData}.
 *
 * <p>Disabled in environments without a running Hazelcast cluster on
 * {@code skyblock-hazelcast-net}, because {@link dev.sbs.simplifieddata.config.PersistenceConfig}
 * blocks on cluster connection during application context startup. Set the environment variable
 * {@code SKYBLOCK_HAZELCAST_DISABLED=true} to skip this test in CI.</p>
 */
@SpringBootTest
@DisabledIfEnvironmentVariable(named = "SKYBLOCK_HAZELCAST_DISABLED", matches = "true")
class SimplifiedDataApplicationTests {

    @Test
    void contextLoads() {
        // Empty body; the @SpringBootTest annotation is the assertion.
        // If the application context fails to start (e.g., Hazelcast cluster unreachable),
        // this test fails with the underlying cause.
    }

}
