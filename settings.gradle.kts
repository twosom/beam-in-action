pluginManagement {
    plugins {
        kotlin("jvm") version "1.9.23"
    }
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "beam-in-action"

listOf(
    "utils",
    "word-count",
    "windowed-word-count",
    "minimal-word-count",
    "window-example",
    "view-example",
    "to-string-example",
    "kafka-streaming",
    "latest-example",
    "big-query-tornadoes",
    "join-example",
    "filter-example",
    "stateful-do-fn",
    "timer-do-fn",
    "custom-windows",
    "test-stream",
    "side-input-pattern",
).forEach { include(it) }


// module settings for beam book
listOf(
    "ch1",
    "ch2",
).forEach { include("beam-book:$it") }
