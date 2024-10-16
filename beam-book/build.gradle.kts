subprojects {
    dependencies {
        if (project.name in listOf("ch1", "ch2", "ch3", "ch4")) {
            println(project.name)
            implementation(project(":beam-book:beam-book-utils"))
        }
    }
}
