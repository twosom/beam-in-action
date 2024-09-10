package com.icloud.rpc

import com.google.common.annotations.VisibleForTesting
import com.icloud.HashKeyOf
import com.icloud.LogUtils
import com.icloud.PipelineUtils
import com.icloud.Tokenize
import com.icloud.extensions.kv
import com.icloud.extensions.logger
import com.icloud.extensions.minutes
import com.icloud.extensions.parDo
import com.icloud.proto.RpcServiceGrpc
import com.icloud.proto.RpcServiceGrpc.RpcServiceBlockingStub
import com.icloud.proto.Service.Request
import com.icloud.proto.Service.RequestList
import com.icloud.rpc.coder.ValueWithTimestampCoder
import com.icloud.rpc.model.ValueWithTimestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.apache.beam.sdk.coders.InstantCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.Logger
import java.net.InetAddress

object RpcParDoStateful : AbstractRpcParDo() {

    interface Options : DefaultOptions {

        @get:Default.Integer(100)
        var batchSize: Int

        @get:Default.Integer(10)
        var maxWaitSec: Int
    }


    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, Options::class.java)

        AutoCloseableServer.of(options.port).use { server ->
            server.server.start()
            val input = pipeline.readInput(options)
            val result = applyRpc(input, options)
            result
                .apply(Reify.timestamps())
                .apply(LogUtils.of())
            storeResult(result, options)
            pipeline.run().waitUntilFinish()
        }
    }

    @VisibleForTesting
    fun applyRpc(
        input: PCollection<String>,
        options: Options,
    ): PCollection<KV<String, Int>> =
        input
            .apply(Tokenize.of())
            .apply(HashKeyOf.of(10))
            .apply(
                BatchRpcDoFnStateful(
                    maxBatchSize = options.batchSize,
                    maxBatchWait = Duration.standardSeconds(options.maxWaitSec.toLong()),
                    hostname = InetAddress.getLocalHost().hostName,
                    port = options.port
                ).parDo()
            )

    @Suppress("UNUSED")
    class BatchRpcDoFnStateful(
        private val maxBatchSize: Int,
        private val maxBatchWait: Duration,
        private val hostname: String,
        private val port: Int,
    ) : DoFn<KV<Int, String>, KV<String, Int>>() {

        @Deprecated("Deprecated in Java", ReplaceWith("10.minutes()", "com.icloud.extensions.minutes"))
        override fun getAllowedTimestampSkew() = 10.minutes()

        companion object {
            private val LOG: Logger = BatchRpcDoFnStateful::class.logger()
        }

        @Transient
        private var channel: ManagedChannel? = null

        @Transient
        private var stub: RpcServiceBlockingStub? = null

        @StateId("batch")
        private val batchSpec: StateSpec<BagState<ValueWithTimestamp<String>>> =
            StateSpecs.bag(ValueWithTimestampCoder(StringUtf8Coder.of()))

        @StateId("batch_size")
        private val batchSizeSpec: StateSpec<ValueState<Int>> =
            StateSpecs.value(VarIntCoder.of())

        @StateId("batch_min_timestamp")
        private val batchMinTimestampSpec: StateSpec<ValueState<Instant>> =
            StateSpecs.value(InstantCoder.of())

        @TimerId("flush_timer")
        private val flushTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

        @TimerId("end_of_time_timer")
        private val endOfTimeTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)

        @Setup
        fun setup() {
            this.channel = ManagedChannelBuilder.forAddress(this.hostname, this.port).usePlaintext().build()
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

        @ProcessElement
        @RequiresTimeSortedInput
        fun process(
            @Element input: KV<Int, String>,
            @Timestamp timestamp: Instant,
            @StateId("batch") batchState: BagState<ValueWithTimestamp<String>>,
            @StateId("batch_size") batchSizeState: ValueState<Int>,
            @StateId("batch_min_timestamp") batchMinTimestampState: ValueState<Instant>,
            @TimerId("flush_timer") flushTimer: Timer,
            @TimerId("end_of_time_timer") endOfTimeTimer: Timer,
            output: OutputReceiver<KV<String, Int>>,
        ) {
            //TODO
            // batch_min_timestamp 를 읽는다.
            // - 만약 아직 없으면 BoundedWindow.TIMESTAMP_MAX_VALUE 로 대체
            var currentMinTimestamp =
                batchMinTimestampState.read() ?: BoundedWindow.TIMESTAMP_MAX_VALUE


            //TODO
            // 읽어온 batch_min_timestamp 를 현재 요소의 timestamp 와 비교
            // - 만약 읽어온 batch_min_timetsamp가 현재 요소의 timestamp 보다 이후이면
            // - 현재 요소의 timestamp로 대체
            // - 즉, min(batch_min_timestamp, timestamp)
            if (currentMinTimestamp.isAfter(timestamp)) {
                currentMinTimestamp = timestamp
                batchMinTimestampState.write(timestamp)
            }

            //TODO
            // 위의 if 브라켓 안에 들어가도 되는것이 아닌지
            endOfTimeTimer.withOutputTimestamp(currentMinTimestamp)
                .set(GlobalWindow.INSTANCE.maxTimestamp())
                .apply { LOG.info("[@ProcessElement] set end_of_time timer at $currentMinTimestamp") }


            //TODO
            // batch_size 를 읽는다.
            // - 만약 아직 없으면 0으로 대체
            var currentSize = batchSizeState.read() ?: 0

            val value = ValueWithTimestamp.of(input.value, timestamp)

            //TODO
            // 만약 [@ProcessElement] 에서 버퍼 임계점에 도달하면
            // 플러시 후 상태 초기화
            if (currentSize.isFull()) {
                this.flushOutput(
                    batchState.read() + value,
                    output
                )
                this.clearState(
                    batchState,
                    batchSizeState,
                    batchMinTimestampState,
                    flushTimer
                )
            }
            //TODO
            // 그게 아닌 경우,
            // - 만약 버퍼 요소가 아직 0인 경우, 플러시 타이머 설정
            // - 버퍼 요소 추가 후 현재 버퍼 갯수 갱신
            else {
                if (currentSize == 0) {
                    flushTimer.withOutputTimestamp(currentMinTimestamp)
                        .offset(maxBatchWait)
                        .setRelative()
                        .apply { LOG.info("[@ProcessElement] flush_timer set with output timestamp $currentMinTimestamp") }
                }
                batchState.add(value)
                currentSize += 1
                batchSizeState.write(currentSize)
                    .apply { LOG.info("[@ProcessElement] current size = $currentSize") }
            }
        }


        private fun Int.isFull() = this == maxBatchSize - 1

        @OnTimer("flush_timer")
        fun onFlushTimer(
            @StateId("batch") batchState: BagState<ValueWithTimestamp<String>>,
            @StateId("batch_size") batchSizeState: ValueState<Int>,
            @StateId("batch_min_timestamp") batchMinTimestampState: ValueState<Instant>,
            output: OutputReceiver<KV<String, Int>>,
        ) {
            LOG.info("[@OnTimer] flush_timer triggered at ${Instant.now()}")
            this.flushOutput(batchState.read(), output)
                .apply { LOG.info("[@OnTimer] flush_timer flushed...") }
            //TODO
            // flush_timer 는 초기화하지 않아도 되는가...?
            this.clearState(batchState, batchSizeState, batchMinTimestampState)
        }

        @OnTimer("end_of_time_timer")
        fun onEndOfTimeTimer(
            @StateId("batch") batchState: BagState<ValueWithTimestamp<String>>,
            output: OutputReceiver<KV<String, Int>>,
        ) {
            this.flushOutput(batchState.read(), output)
                .apply { LOG.info("[@OnTimer] end_of_time_timer flushed...") }
        }

        @OnWindowExpiration
        fun onWindowExpiration() {
            LOG.info("[@OnWindowExpiration] triggered at {}", Instant.now())
        }

        private fun flushOutput(
            elements: Iterable<ValueWithTimestamp<String>>,
            output: OutputReceiver<KV<String, Int>>,
        ) {
            val distinctElements: Map<String, List<ValueWithTimestamp<String>>> =
                elements.groupBy { it.value }

            val requestList = RequestList.newBuilder()
                .let { builder ->
                    distinctElements.keys.forEach { builder.addRequest(Request.newBuilder().setInput(it)) }
                    builder.build()
                }

            val responseList = this.stub!!.resolveBatch(requestList)

            check(requestList.requestCount == responseList.responseCount) { "Request count should be equal to Response count" }

            for (i in 0 until requestList.requestCount) {
                val request = requestList.getRequest(i)
                val response = responseList.getResponse(i)
                val timestampsAndWindows = distinctElements[request.input] ?: listOf()
                val value = request.input kv response.output
                timestampsAndWindows
                    .sortedBy { it.timestamp }
                    .forEach { output.outputWithTimestamp(value, it.timestamp) }
            }
        }

        private fun clearState(
            vararg states: Any,
        ) = states.forEach {
            when (it) {
                is State -> it.clear()
                    .apply { LOG.info("[@ClearState] state cleared...") }

                is Timer -> it.clear()
                    .apply { LOG.info("[@ClearState] timer cleared...") }
            }
        }

    }


}