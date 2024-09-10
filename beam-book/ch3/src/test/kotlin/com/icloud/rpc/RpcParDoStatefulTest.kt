package com.icloud.rpc

import com.icloud.HasInput
import com.icloud.OptionUtils
import com.icloud.Utils
import com.icloud.extensions.kv
import com.icloud.rpc.AutoCloseableServer
import com.icloud.rpc.RpcParDoStateful
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
class RpcParDoStatefulTest : HasInput() {

    @Rule
    @JvmField
    val pipeline: TestPipeline = TestPipeline.create()

    companion object {
        private const val PORT = 1234
        private lateinit var server: AutoCloseableServer

        @JvmStatic
        @BeforeClass
        fun setUp() {
            server = AutoCloseableServer.of(PORT)
                .also { it.server.start() }
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            server.close()
        }
    }

    @Test
    fun `test RPC`() {
        val lines = this.pipeline.apply(Create.of(input))

        val result = RpcParDoStateful.applyRpc(
            lines,
            OptionUtils.createOption(
                arrayOf(
                    "--port=$PORT",
                    "--batchSize=10",
                    "--maxWaitSec=1",
                    "--bootstrapServer=null",
                    "--inputTopic=null",
                    "--outputTopic=null",
                ),
                RpcParDoStateful.Options::class.java
            )
        )

        val expectedResult = input.flatMap { Utils.toWords(it) }
            .map { it kv it.length }

        PAssert.that(result)
            .containsInAnyOrder(expectedResult)

        pipeline.run().waitUntilFinish()
    }


}