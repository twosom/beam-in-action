package com.icloud.rpc

import com.google.common.annotations.VisibleForTesting
import com.icloud.LogUtils
import com.icloud.PipelineUtils
import com.icloud.Tokenize
import com.icloud.extensions.kv
import com.icloud.extensions.logger
import com.icloud.extensions.parDo
import com.icloud.proto.RpcServiceGrpc
import com.icloud.proto.RpcServiceGrpc.RpcServiceBlockingStub
import com.icloud.proto.Service.Request
import com.icloud.proto.Service.RequestList
import com.icloud.rpc.model.ValueWithTimestampAndWindow
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant
import org.slf4j.Logger
import java.net.InetAddress

object RpcParDoBatch : AbstractRpcParDo() {

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, DefaultOptions::class.java)
        AutoCloseableServer.of(options.port).use { server ->
            server.server.start()
            val input = pipeline.readInput(options)
            val result = applyRpc(input, options.port)

            result.apply(Reify.timestamps())
                .apply(LogUtils.of())

            storeResult(result, options)
            pipeline.run().waitUntilFinish()
        }
    }

    @VisibleForTesting
    fun applyRpc(
        input: PCollection<String>,
        port: Int,
    ): PCollection<KV<String, Int>> {
        val hostAddress = InetAddress.getLocalHost().hostAddress
        return input.apply(Tokenize.of())
            .apply(
                BatchRpcDoFn(
                    hostname = hostAddress,
                    port = port
                ).parDo()
            )
    }

    class BatchRpcDoFn(
        private val hostname: String,
        private val port: Int,
    ) : DoFn<String, KV<String, Int>>() {

        companion object {
            private val LOG: Logger = BatchRpcDoFn::class.logger()
        }

        @Transient
        private var channel: ManagedChannel? = null

        @Transient
        private var stub: RpcServiceBlockingStub? = null

        @Transient
        private lateinit var elements: MutableList<ValueWithTimestampAndWindow<String>>

        @Setup
        fun setup() {
            this.channel = ManagedChannelBuilder.forAddress(this.hostname, this.port).usePlaintext().build()
                .also { LOG.info("channel opened on port $port") }
            this.stub = RpcServiceGrpc.newBlockingStub(this.channel)
        }

        @Teardown
        fun tearDown() {
            if (channel != null) {
                channel!!.shutdown()
                    .apply { LOG.info("[@Teardown] channel shutdown...") }
                channel = null
                LOG.info("[@Teardown] channel shutdown completed...")
            }
        }

        @StartBundle
        fun startBundle() {
            this.elements = mutableListOf<ValueWithTimestampAndWindow<String>>()
                .apply { LOG.info("[@StartBundle] buffer initialized...") }
        }

        @ProcessElement
        fun processElement(
            @Element input: String,
            @Timestamp timestamp: Instant,
            window: BoundedWindow,
        ) {
            this.elements.add(ValueWithTimestampAndWindow(input, timestamp, window))
        }

        @FinishBundle
        fun finishBundle(
            context: FinishBundleContext,
        ) {
            val builder = RequestList.newBuilder()
            val distinctElements = elements.groupBy { it.value }
            val requestList =
                distinctElements.keys
                    .forEach { builder.addRequest(Request.newBuilder().setInput(it)) }
                    .let { builder.build() }

            val responseList = this.stub!!.resolveBatch(requestList)

            check(requestList.requestCount == responseList.responseCount) { "Request count should be equal to Response count" }

            for (i in 0 until requestList.requestCount) {
                val request = requestList.getRequest(i)
                val response = responseList.getResponse(i)
                val timestampAndWindow: List<ValueWithTimestampAndWindow<String>> =
                    distinctElements[request.input] ?: listOf()
                val value = request.input kv response.output
                timestampAndWindow.forEach { context.output(value, it.timestamp, it.window) }
                    .also { LOG.info("[@FinishBundle] bundle output completed at {}", Instant.now()) }
            }
        }
    }

}