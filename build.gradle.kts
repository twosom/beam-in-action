import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

apply {
    from("gradle/beam.gradle")
}

setProperty("mainClassName", "none")

plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val beamVersion: String = "2.56.0"

group = "com.icloud"
version = "1.0-SNAPSHOT"

subprojects {
    group = "com.icloud"
    version = "1.0-SNAPSHOT"

    plugins.apply {
        apply("java")
        apply("application")
        apply("com.github.johnrengelman.shadow")
    }

    repositories {
        mavenCentral()
        maven {
            url = uri("https://repository.apache.org/content/repositories/snapshots/")
        }
        maven {
            url = uri("https://repo.maven.apache.org/maven2/")
        }
        maven {
            url = uri("https://packages.confluent.io/maven/")
        }
    }
    dependencies {
        // beam bom
        implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:$beamVersion"))

        // Flink fasterxml dependencies...
        implementation("com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.16.1")
        implementation("com.fasterxml.jackson.datatype:jackson-datatype-joda:2.16.1")

        // hadoop
        implementation("org.apache.hadoop:hadoop-common:3.3.6")
        implementation("org.apache.hadoop:hadoop-hdfs-client:3.3.6")

        // bigquery
        implementation("com.google.apis:google-api-services-bigquery:v2-rev459-1.25.0")


        // beam implementation
        beamImplementation(
            "beam-sdks-java-core",
            "beam-runners-direct-java",
            "beam-sdks-java-extensions-google-cloud-platform-core",
            "beam-sdks-java-io-kafka",
            "beam-sdks-java-io-hadoop-file-system",
            "beam-sdks-java-extensions-join-library"
        )
        implementation("org.apache.kafka:kafka-clients:3.4.0")

        beamRuntimeOnly(
            "beam-sdks-java-io-google-cloud-platform",
            "beam-runners-google-cloud-dataflow-java",
        )

        // logger
        implementation("org.slf4j:slf4j-jdk14:1.7.32")
        // lombok
        compileOnly("org.projectlombok:lombok:1.18.20")
        annotationProcessor("org.projectlombok:lombok:1.18.20")

        // auto value
        compileOnly("com.google.auto.value:auto-value-annotations")
        annotationProcessor("com.google.auto.value:auto-value:1.10.4")

        testImplementation(platform("org.junit:junit-bom:5.9.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testImplementation("org.mockito:mockito-core:2.1.0")
        testImplementation("org.junit.vintage:junit-vintage-engine")
        testImplementation("junit:junit:4.13.2")

        if (project.name != "utils") {
            implementation(project(":utils"))
        }
    }
}

allprojects {
    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    tasks {
        test {
            useJUnitPlatform()
        }
        compileJava {
            options.compilerArgs.add("-parameters")
        }
    }

    tasks.withType<ShadowJar> {
        isZip64 = true
        mergeServiceFiles()
    }
}

fun DependencyHandlerScope.beamImplementation(vararg args: String) {
    for (arg in args) {
        implementation("org.apache.beam:${arg}")
    }
}

fun DependencyHandlerScope.beamRuntimeOnly(vararg args: String) {
    for (arg in args) {
        runtimeOnly("org.apache.beam:${arg}")
    }
}

