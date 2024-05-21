package com.icloud

import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.icloud.extensions.clear
import com.icloud.extensions.kv
import com.icloud.extensions.parDo
import com.icloud.extensions.tfs
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.math.BigDecimal

object TimerDoFn {
    private val LOG = LoggerFactory.getLogger(TimerDoFn::class.java)

    interface Options : PipelineOptions {
        @get:Description("Table to write to")
        @get:Validation.Required
        var table: String

        @get:Description("Topic to read from")
        @get:Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        var topic: String
    }

    private val SCHEMA: TableSchema by lazy {
        TableSchema().setFields(
            listOf(
                "ride_status" tfs "STRING",
                "meter_reading" tfs "FLOAT",
                "timestamp" tfs "TIMESTAMP",
                "passenger_counter" tfs "INTEGER"
            )
        )
    }


    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, Options::class.java)

        pipeline.apply(
            "Read From PubSub",
            PubsubIO.readStrings()
                .fromTopic(options.topic)
        )
            .apply(
                "Parse and to KV",
                object : DoFn<String, KV<String, String>>() {

                    @ProcessElement
                    fun process(
                        @Element message: String,
                        output: OutputReceiver<KV<String, String>>,
                    ) = run {
                        val rideStatus =
                            JSONObject(message)["ride_status"] as? String ?: return
                        output.output(rideStatus kv message)
                    }
                }.parDo()
            )
            .apply(
                "Timer",
                object : DoFn<KV<String, String>, TableRow>() {

                    private val BUFFER_TIME: Duration = Duration.standardSeconds(100)

                    @TimerId("timer")
                    private val timerSpec: TimerSpec =
                        TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

                    @StateId("counter")
                    private val counter: StateSpec<ValueState<Int>> =
                        StateSpecs.value()

                    @StateId("buffer")
                    private val bufferedEvents: StateSpec<BagState<TableRow>> =
                        StateSpecs.bag()


                    @ProcessElement
                    fun process(
                        @Element element: KV<String, String>,
                        @TimerId("timer") timer: Timer,
                        @StateId("counter") counter: ValueState<Int>,
                        @StateId("buffer") buffer: BagState<TableRow>,
                    ) = run {
                        var count = counter.read() ?: 0
                        if (count == 0) {
                            timer.offset(BUFFER_TIME).setRelative()
                            LOG.info("TIMER: Setting timer at {}", Instant.now().toString())
                        }

                        val json = JSONObject(element.value)
                        val timestamp = json["timestamp"] as? String ?: return
                        val meterReading = json["meter_reading"] as? BigDecimal ?: return
                        val passengerCount = json["passenger_count"] as? Int ?: return

                        val row = TableRow()
                        row["ride_status"] = element.key
                        row["timestamp"] = timestamp
                        row["meter_reading"] = meterReading
                        row["passenger_count"] = passengerCount

                        buffer.add(row)
                        count += 1
                        counter.write(count)
                    }

                    @OnTimer("timer")
                    fun onTimer(
                        @StateId("counter") counter: ValueState<Int>,
                        @StateId("buffer") buffer: BagState<TableRow>,
                        output: OutputReceiver<TableRow>,
                    ) = run {
                        LOG.info(
                            "TIMER: Releasing buffer at {} with {} elements",
                            Instant.now().toString(),
                            counter.read()
                        )

                        buffer.read()
                            .forEach { output.output(it) }
                            .apply {
                                counter.clear("Counter cleared...")
                                buffer.clear("Buffer cleared...")
                            }
                    }

                }.parDo()
            )
            .apply(
                "WriteIntoBigQuery",
                BigQueryIO.writeTableRows()
                    .to(options.table)
                    .withSchema(SCHEMA)
                    .withFailedInsertRetryPolicy(
                        InsertRetryPolicy.neverRetry()
                    )
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            )


        pipeline.run().waitUntilFinish()
    }
}