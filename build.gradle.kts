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

    // Spring Boot
    implementation(libs.spring.boot.starter)
    implementation(libs.spring.boot.starter.actuator)

    // Hazelcast - this module opts into JpaCacheProvider.HAZELCAST_CLIENT, so the
    // persistence library's compileOnly Hazelcast dep must be promoted to runtimeOnly here.
    // The testRuntimeOnly mirror is required by JpaModelHazelcastTest (verification step 5.9)
    // because Gradle's runtimeOnly does NOT cascade into testRuntimeClasspath by default.
    runtimeOnly(libs.hazelcast)
    testRuntimeOnly(libs.hazelcast)

    // Projects
    implementation("dev.sbs:minecraft-api:0.1.0")
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
