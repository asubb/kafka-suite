import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

object Version {
    val kafka = "2.2.0"
    val spek = "2.0.1"
    val jackson = "2.9.4"
    val assertk = "0.13"
}

plugins {
    kotlin("jvm") version "1.3.31"
}

apply<ApplicationPlugin>()
apply<KotlinPluginWrapper>()

repositories {
    jcenter()
    mavenCentral()
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}

val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    implementation("com.fasterxml.jackson.core:jackson-core:${Version.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${Version.jackson}")

    implementation("org.apache.kafka:kafka_2.12:${Version.kafka}")
    implementation("org.apache.kafka:kafka-clients:${Version.kafka}")

    implementation("commons-cli:commons-cli:1.4")

    implementation("io.github.microutils:kotlin-logging:1.7.2")
    implementation("ch.qos.logback:logback-classic:1.2.3")

    testImplementation("org.spekframework.spek2:spek-dsl-jvm:${Version.spek}")
    testRuntimeOnly("org.spekframework.spek2:spek-runner-junit5:${Version.spek}")
    testImplementation("com.willowtreeapps.assertk:assertk-jvm:${Version.assertk}")

}

tasks.test {
    useJUnitPlatform {
        includeEngines("spek2")
    }
}

configure<ApplicationPluginConvention> {
    applicationName = properties["applicationName"]?.toString() ?: "<NO NAME>"
    mainClassName = "kafka.suite.KafkaSuiteKt"
}

tasks.jar {
    manifest {
        attributes(
                "KSuite-Version" to properties["version"],
                "KSuite-Name" to properties["applicationName"]
        )
    }
}