ext {
    set("sparkVersion", "3.4.1")
    set("scalaVersion", "2.12")
}

dependencies {
    runtimeOnly("org.apache.beam:beam-runners-spark-3:2.60.0")
    // spark
    listOf(
        "spark-core",
        "spark-sql",
        "spark-mllib",
        "spark-streaming",
        "spark-network-common",
        "spark-catalyst",
    ).forEach { spark(it) }
}


tasks.register("testRun", JavaExec::class.java) {
    group = "application" // 작업 그룹 설정
    description = "Run the FileChunkReadPipeline application" // 작업 설명 설정

    // 메인 클래스 설정
    mainClass.set("com.icloud.FileChunkReadPipeline")

    // 클래스패스 설정 (예: 프로젝트의 컴파일된 클래스와 의존성 포함)
    classpath = sourceSets.main.get().runtimeClasspath


    // JVM 옵션 설정 (필요한 경우)
    jvmArgs = listOf("-Xmx1024m")
}

fun DependencyHandlerScope.spark(lib: String) {
    val scalaVersion: String by project.ext.properties
    val sparkVersion: String by project.ext.properties
    implementation("org.apache.spark:${lib}_$scalaVersion:$sparkVersion")
}
