rootProject.name = "beam-in-action"

listOf(
    "utils",
    "word-count",
    "windowed-word-count"
).forEach { include(it) }
