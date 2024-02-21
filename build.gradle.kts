plugins {
    id("java")
}

group = "com.codehunter.kafka_pure_playground"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    // Logging
    implementation("org.apache.logging.log4j:log4j-api:2.22.1")
    implementation("org.apache.logging.log4j:log4j-core:2.22.1")

    // Test
    implementation("org.assertj:assertj-core:3.25.3")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}