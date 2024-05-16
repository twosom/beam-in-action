package com.icloud

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TypeDescriptors.strings


object JoinExample {

    private const val GDELT_EVENTS_TABLE: String =
        "apache-beam-testing.samples.gdelt_sample"

    private const val COUNTRY_CODES: String =
        "gdelt-bq:full.crosswalk_geocountrycodetohuman"

    interface Options : PipelineOptions {
        @get:Description("Path of the file to write to")
        @get:Required
        var output: String
    }

    @Suppress("UNUSED")
    class ExtractEventDataFn
        : DoFn<TableRow, KV<String, String>>() {

        @ProcessElement
        fun process(
            @Element row: TableRow,
            output: OutputReceiver<KV<String, String>>,
        ) = run {
            val countryCode: String = row["ActionGeo_CountryCode"] as? String ?: return
            val sqlDate = row["SQLDATE"] as? String ?: return
            val actor1Name = row["Actor1Name"] as? String ?: return
            val sourceUrl = row["SOURCEURL"] as? String ?: return
            val eventInfo = "Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"
            output.output(
                countryCode kv eventInfo
            )
        }
    }

    @Suppress("UNUSED")
    class ExtractCountryInfoFn
        : DoFn<TableRow, KV<String, String>>() {
        @ProcessElement
        fun process(
            @Element row: TableRow,
            output: OutputReceiver<KV<String, String>>,
        ) = run {
            val countryCode = row["FIPSCC"] as? String ?: return
            val countryName = row["HumanName"] as? String ?: return
            output.output(
                countryCode kv countryName
            )
        }

    }


    private fun joinEvents(
        events: PCollection<TableRow>,
        countryCodes: PCollection<TableRow>,
    ): PCollection<String> {
        val eventInfoTag = TupleTag<String>("event-info")
        val countryInfoTag = TupleTag<String>("country-info")

        val eventInfo: PCollection<KV<String, String>> =
            events.apply(ParDo.of(ExtractEventDataFn()))

        val countryInfo: PCollection<KV<String, String>> =
            countryCodes.apply(ParDo.of(ExtractCountryInfoFn()))

        // join with KV's key (current key is country code)
        val joinedCollection = KeyedPCollectionTuple.of(eventInfoTag, eventInfo)
            .and(countryInfoTag, countryInfo)
            .apply(CoGroupByKey.create())


        val resultCollection = joinedCollection.apply(
            "Process",
            ParDo.of(
                object : DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                    @Suppress("UNUSED")
                    @ProcessElement
                    fun process(
                        @Element element: KV<String, CoGbkResult>,
                        output: OutputReceiver<KV<String, String>>,
                    ) = run {
                        val countryCode = element.key
                        val countryName = element.value.getOnly(countryInfoTag)
                        element.value.getAll(eventInfoTag).forEach {
                            output.output(
                                countryCode kv "Country name:  $countryName, Event info : $it"
                            )
                        }
                    }
                }
            )
        )

        return resultCollection.apply(
            "Formatting",
            MapElements.into(strings())
                .via(ProcessFunction { "Country Code: ${it.key}, ${it.value}" })
        )
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, Options::class.java)

        val eventsTable =
            pipeline.apply(
                BigQueryIO.readTableRows()
                    .withMethod(Method.DIRECT_READ)
                    .from(GDELT_EVENTS_TABLE)
            )

        val countryCodes =
            pipeline.apply(
                BigQueryIO.readTableRows()
                    .withMethod(Method.DIRECT_READ)
                    .from(COUNTRY_CODES)
            )

        val formattedResults = joinEvents(eventsTable, countryCodes)

        formattedResults.apply(TextIO.write().to(options.output))

        pipeline.run().waitUntilFinish()
    }
}