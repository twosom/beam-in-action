package com.icloud.rpc

import com.icloud.HasInput
import com.icloud.Utils
import com.icloud.extensions.kv
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class RpcParDoBatchTest : HasInput() {

    @Rule
    @JvmField
    var pipeline: TestPipeline = TestPipeline.create()

    companion object {
        private lateinit var server: AutoCloseableServer
        private const val PORT = 1234

        @JvmStatic
        @BeforeClass
        fun before() {
            server = AutoCloseableServer.of(PORT)
            server.server.start()
        }

        @JvmStatic
        @AfterClass
        fun after() {
            server.close()
        }
    }

    @Test
    fun `Test RPC`() {
        val lines = this.pipeline.apply(Create.of(input))

        val result = RpcParDoBatch.applyRpc(lines, PORT)

        val expectedResult =
            input.flatMap { Utils.toWords(it) }
                .map { it kv it.length }


        PAssert.that(result).containsInAnyOrder(expectedResult)

        this.pipeline.run()

    }
}