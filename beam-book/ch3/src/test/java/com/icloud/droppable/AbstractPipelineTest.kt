package com.icloud.droppable

import org.apache.beam.sdk.testing.TestPipeline
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
abstract class AbstractPipelineTest {
    @Rule
    @JvmField
    val p: TestPipeline = TestPipeline.create()
}