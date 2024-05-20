package com.icloud

import com.icloud.extensions.clear
import com.icloud.extensions.kv
import com.icloud.extensions.parDo
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.state.BagState
import org.apache.beam.sdk.state.StateSpec
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.joda.time.Instant
import org.json.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * Pipeline for use Stateful Do Fn
 */
object StatefulDoFn {

    /**
     * data class for describe taxi ride info
     */
    @DefaultCoder(AvroCoder::class)
    data class TaxiRideInfo(
        val rideId: String,
        val pointIdx: Int,
        val latitude: Float,
        val longitude: Float,
        val timestamp: Instant,
        val meterReading: Double,
        val meterIncrement: Double,
        val rideStatus: String,
        val passengerCount: Int,
    ) : Serializable {
        /*NOTE dummy no args constructor for serialize*/
        @Suppress("UNUSED")
        constructor() : this(
            "",
            0,
            Float.MIN_VALUE,
            Float.MIN_VALUE,
            Instant.now(),
            .0,
            .0,
            "",
            0
        )
    }

    interface Options : PipelineOptions {
        @get:Description("The topic of read from Pub/Sub")
        @get:Required
        @get:Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        var topic: String
    }


    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, Options::class.java)

        pipeline.apply(
            "Read Taxi data form PubSub",
            PubsubIO.readMessages()
                .fromTopic(options.topic)
        )
            .apply(
                "Convert topic data to Serialized Object",
                ParsePubSubMessageFn().parDo()
            )
            .apply(
                "Group By ride status",
                MapElements.into(kvs(strings(), object : TypeDescriptor<TaxiRideInfo>() {}))
                    .via(SerializableFunction { it.rideStatus kv it })
            )
            .apply(
                "Buffer with state",
                BufferFn().parDo()
            )
            .apply(Reify.timestamps())
            .apply(LogUtils.of())
        pipeline.run().waitUntilFinish()

    }

    @Suppress("UNUSED")
    class BufferFn : DoFn<KV<String, TaxiRideInfo>, KV<String, TaxiRideInfo>>() {

        companion object {
            private const val MAX_BUFFER_SIZE_ENROUTE = 1_000
            private const val MAX_BUFFER_SIZE = 100
            private val LOG: Logger = LoggerFactory.getLogger(BufferFn::class.java)
        }

        @StateId("counter")
        private val counterSpec: StateSpec<ValueState<Int>> = StateSpecs.value()

        @StateId("buffer")
        private val bufferSpec: StateSpec<BagState<TaxiRideInfo>> =
            StateSpecs.bag(AvroCoder.of(TaxiRideInfo::class.java))

        @ProcessElement
        fun process(
            @Element element: KV<String, TaxiRideInfo>,
            @StateId("counter") counterState: ValueState<Int>,
            @StateId("buffer") bufferState: BagState<TaxiRideInfo>,
            output: OutputReceiver<KV<String, TaxiRideInfo>>,
        ) = run {
            // get counter and update
            var counter = counterState.read() ?: 0
            counter += 1
            LOG.info("counter = {}", counter)
            counterState.write(counter)

            // add elements to buffer
            bufferState.add(element.value)

            val isEnroute = counter >= MAX_BUFFER_SIZE_ENROUTE && element.key == "enroute"
            val isNotEnroute = counter >= MAX_BUFFER_SIZE && element.key != "enroute"

            if (isEnroute || isNotEnroute) {
                bufferState.read().forEach { output.output(it.rideStatus kv it) }
                    .apply {
                        counterState.clear("Counter state cleared...")
                        bufferState.clear("Buffer state cleared...")
                    }
            }
        }

    }

    class ParsePubSubMessageFn : DoFn<PubsubMessage, TaxiRideInfo>() {
        @ProcessElement
        @Suppress("UNUSED")
        fun process(
            @Element message: PubsubMessage,
            output: OutputReceiver<TaxiRideInfo>,
        ) = run {
            val payloadJsonString = message.payload.decodeToString()
            val json = JSONObject(payloadJsonString)
            output.output(
                TaxiRideInfo(
                    json.getString("ride_id"),
                    json.getInt("point_idx"),
                    json.getFloat("latitude"),
                    json.getFloat("longitude"),
                    Instant.parse(json.getString("timestamp")),
                    json.getDouble("meter_reading"),
                    json.getDouble("meter_increment"),
                    json.getString("ride_status"),
                    json.getInt("passenger_count")
                )
            )
        }
    }
}