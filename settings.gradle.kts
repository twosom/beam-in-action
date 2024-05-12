rootProject.name = "beam-in-action"

listOf(
    "utils",
    "word-count",
    "windowed-word-count",
    "minimal-word-count",
    "window-example",
    "view-example",
    "to-string-example",
).forEach { include(it) }
