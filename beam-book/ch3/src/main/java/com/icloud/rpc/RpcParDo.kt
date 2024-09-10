package com.icloud.rpc

import com.google.common.annotations.VisibleForTesting
import com.icloud.PipelineUtils
import com.icloud.Tokenize
import com.icloud.extensions.logger
import com.icloud.extensions.parDo
import com.icloud.proto.RpcServiceGrpc
import com.icloud.proto.RpcServiceGrpc.RpcServiceBlockingStub
import com.icloud.proto.Service
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.slf4j.Logger
import java.net.InetAddress

object RpcParDo : AbstractRpcParDo() {


    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, DefaultOptions::class.java)
        AutoCloseableServer.of(options.port).use { server ->
            server.server.start()
            val input = pipeline.readInput(options)
            val result = applyRpc(input, options.port)
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
                RpcDoFn(
                    hostname = hostAddress,
                    port = port
                ).parDo()
            )
    }


    class RpcDoFn(
        private val hostname: String,
        private val port: Int,
    ) : DoFn<String, KV<String, Int>>() {

        companion object {
            private val LOG: Logger = RpcDoFn::class.logger()
        }

        @Transient
        private var channel: ManagedChannel? = null

        @Transient
        private var stub: RpcServiceBlockingStub? = null

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
                channel = null
                LOG.info("[@TearDown] Channel shutdown completed...")
            }
        }

        @ProcessElement
        fun processElement(
            @Element input: String,
            output: OutputReceiver<KV<String, Int>>,
        ) {
            val response = this.stub!!.resolve(Service.Request.newBuilder().setInput(input).build())
            output.output(KV.of(input, response.output))
        }

    }
}