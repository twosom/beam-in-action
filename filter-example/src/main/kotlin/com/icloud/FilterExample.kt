package com.icloud

import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.icloud.extensions.*
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Mean
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView

object FilterExample {

    private const val WEATHER_SAMPLE_TABLE = "apache-beam-testing.samples.weather_stations"
    private const val MONTH_TO_FILTER = 7
    private val SCHEMA: TableSchema by lazy {
        TableSchema().setFields(
            arrayListOf(
                "year" tfs "INTEGER",
                "month" tfs "INTEGER",
                "day" tfs "INTEGER",
                "mean_temp" tfs "FLOAT"
            )
        )
    }

    interface Options : PipelineOptions {

        @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Default.String(WEATHER_SAMPLE_TABLE)
        var input: String

        @get:Description("Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist")
        @get:Required
        var output: String


        @get:Description("Numeric value of month to filter on")
        @get:Default.Integer(MONTH_TO_FILTER)
        var monthFilter: Int

        @get:Description("The method for read BigQuery")
        @get:Default.Enum("DIRECT_READ")
        var readMethod: TypedRead.Method

        @get:Description("The Temp location for save to Big Query")
        @get:Required
        var resultTempLocation: ValueProvider<String>
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, Options::class.java)

        pipeline.apply(
            "Read From BigQuery",
            BigQueryIO.readTableRows().from(options.input)
                .withMethod(options.readMethod)
        )
            .apply(ProjectionFn().parDo())
            .apply(BelowGlobalMean(options.monthFilter))
            .apply(LogUtils.of())
            .apply(
                BigQueryIO.writeTableRows()
                    .to(options.output)
                    .withSchema(SCHEMA)
                    .withCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(Write.WriteDisposition.WRITE_TRUNCATE)
                    .withCustomGcsTempLocation(options.resultTempLocation)
            )

        pipeline.run().waitUntilFinish()
    }

    class ProjectionFn :
        DoFn<TableRow, TableRow>() {

        @Suppress("UNUSED")
        @ProcessElement
        fun process(
            @Element row: TableRow,
            output: OutputReceiver<TableRow>,
        ) = run {
            val year = row.int("year") ?: return
            val month = row.int("month") ?: return
            val day = row.int("day") ?: return
            val meanTemp = row.double("mean_temp") ?: return

            val outRow = TableRow()
                .set("year", year)
                .set("month", month)
                .set("day", day)
                .set("mean_temp", meanTemp)
            output.output(outRow)
        }
    }

    class BelowGlobalMean(
        private val monthFilter: Int?,
    ) : PTransform<PCollection<TableRow>, PCollection<TableRow>>() {

        override fun expand(
            rows: PCollection<TableRow>,
        ): PCollection<TableRow> {
            val meanTemps =
                rows.apply(ExtractTempFn().parDo())

            val globalMeanTemp =
                meanTemps.apply(Mean.globally()).singletonView()

            val monthFilteredRows =
                rows.apply(FilterSingleMonthDateFn(this.monthFilter).parDo())

            return monthFilteredRows.apply(
                "ParseAndFilter",
                ParseAndFilterFn(globalMeanTemp).parDo()
                    .withSideInputs(globalMeanTemp)
            )
        }

    }

    class ParseAndFilterFn(
        private val globalMeanTemp: PCollectionView<Double>,
    ) : DoFn<TableRow, TableRow>() {
        @ProcessElement
        @Suppress("UNUSED")
        fun process(
            @Element row: TableRow,
            output: OutputReceiver<TableRow>,
            context: ProcessContext,
        ) = run {
            val meanTemp = row.double("mean_temp") ?: return
            val gTemp = context.sideInput(globalMeanTemp)
            if (meanTemp < gTemp) {
                output.output(row)
            }
        }
    }

    class FilterSingleMonthDateFn(
        private val monthFilter: Int?,
    ) : DoFn<TableRow, TableRow>() {
        @Suppress("UNUSED")
        @ProcessElement
        fun process(
            @Element row: TableRow,
            output: OutputReceiver<TableRow>,
        ) = run {
            val month = row.int("month") ?: return
            if (month == monthFilter) {
                output.output(row)
            }
        }
    }


    class ExtractTempFn : DoFn<TableRow, Double>() {
        @Suppress("UNUSED")
        @ProcessElement
        fun element(
            @Element row: TableRow,
            output: OutputReceiver<Double?>,
        ) = run {
            output.output(row.double("mean_temp"))
        }
    }


}

