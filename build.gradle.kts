plugins {
    id("java")
    id("com.gradleup.shadow") version "8.3.6"
}

group = "dev.sbs"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    maven(url = "https://jitpack.io")
}

dependencies {
    // Simplified Annotations
    annotationProcessor(libs.simplified.annotations)

    // Lombok Annotations
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Tests
    testImplementation(libs.hamcrest)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.junit.platform.launcher)
    testImplementation(libs.spring.boot.starter.test)

    // Server framework - transitively provides spring-boot-starter-web and
    // spring-boot-starter-actuator via api() exports. Phase 6b.3 swapped the
    // previous spring-boot-starter + spring-boot-starter-actuator pair for this
    // single dep so the Spring setup matches simplified-server / server-api, and
    // simplified-data picks up the servlet container required for the
    // /actuator/prometheus scrape endpoint. API key authentication is disabled
    // in application.properties because simplified-data exposes no REST
    // endpoints to protect.
    implementation("dev.sbs:server-api:0.1.0")

    // Micrometer Prometheus registry - Phase 6b.3. Version pinned explicitly via
    // the catalog to avoid drift against Spring Boot's managed dependencies
    // since simplified-data does not import spring-boot-dependencies as a BOM.
    implementation(libs.micrometer.registry.prometheus)

    // Hazelcast - promoted from runtimeOnly to implementation in Phase 6b because
    // PersistenceConfig now references HazelcastInstance + HazelcastClient directly
    // for the write-path bean, the WriteQueueConsumer uses IQueue<WriteRequest>
    // as its drain entry point, and the WriteBatchScheduler iterates the registry
    // via an IMap for the dead-letter dump. Earlier phases only used Hazelcast
    // indirectly through the JCache SPI which is why runtimeOnly was sufficient.
    implementation(libs.hazelcast)

    // Projects
    implementation("dev.sbs:minecraft-api:0.1.0")
    testImplementation("dev.sbs:asset-renderer:0.1.0")
}

tasks {
    withType<JavaCompile> {
        options.compilerArgs.add("-parameters")
    }

    test {
        useJUnitPlatform()
    }

    shadowJar {
        archiveClassifier.set("")
        mergeServiceFiles()
        append("META-INF/spring.factories")
        append("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")

        manifest {
            attributes["Main-Class"] = "dev.sbs.simplifieddata.SimplifiedData"
            attributes["Multi-Release"] = "true"
        }

        exclude("META-INF/INDEX.LIST", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }

    build {
        dependsOn(shadowJar)
    }

    register<Exec>("deploy") {
        description = "Build and deploy to remote Docker host via SSH over VPN"
        group = "deployment"
        dependsOn(shadowJar)

        doFirst {
            project.file(".env").readLines()
                .filter { it.contains('=') && !it.startsWith('#') }
                .forEach { environment(it.substringBefore('='), it.substringAfter('=')) }
        }

        commandLine("docker", "compose", "up", "-d", "--build", "--remove-orphans")
    }
}
