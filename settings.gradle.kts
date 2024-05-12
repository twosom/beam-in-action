rootProject.name = "beam-in-action"

listOf(
    "utils",
    "word-count",
    "windowed-word-count",
    "minimal-word-count",
    "window-example",
    "view-example"
).forEach { include(it) }
