plugins {
    id("java")
}

group = "com.codehunter.kafka_pure_playground"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    val main by getting
    val test by getting

    val intTest by creating {
        compileClasspath += main.output + test.output
        runtimeClasspath += main.output + test.output
    }
}

configurations {
    val testImplementation by getting
    val testRuntimeOnly by getting

    "intTestImplementation" {
        extendsFrom(testImplementation)
    }
    "intTestRuntimeOnly" {
        extendsFrom(testRuntimeOnly)
    }
}

dependencies {

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    // Logging
    implementation("org.apache.logging.log4j:log4j-api:2.22.1")
    implementation("org.apache.logging.log4j:log4j-core:2.22.1")
    // Caching
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")

    // Test
    implementation("org.assertj:assertj-core:3.25.3")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.awaitility:awaitility:4.2.0")

    // Testcontainers
    // https://central.sonatype.com/artifact/org.testcontainers/testcontainers-bom
    implementation(platform("org.testcontainers:testcontainers-bom:1.19.6"))
    testImplementation("org.testcontainers:kafka")
}

tasks.test {
    useJUnitPlatform()
}