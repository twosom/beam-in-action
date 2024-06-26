package com.icloud.extensions

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.state.State
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Trigger
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Instant

/**
 * infix method for create KV
 */
infix fun <K, V> K.kv(v: V): KV<K, V> = KV.of(this, v)

/**
 * infix method for create TableFieldSchema
 * @param type type of table field schema.
 * @see TableFieldSchema.setType
 */
infix fun String.tfs(type: String): TableFieldSchema =
    TableFieldSchema().setName(this).setType(type)


/**
 * extract property and convert to Integer type (nullable)
 * @param property to extract property
 */
fun TableRow.int(property: String): Int? =
    this[property]?.toString()?.toInt()

/**
 * extract property and convert to Double type (nullable)
 * @param property to extract property
 */
fun TableRow.double(property: String): Double? =
    this[property]?.toString()?.toDouble()

/**
 * make ParDo from DoFn
 */
fun <InputT, OutputT, FnT : DoFn<InputT, OutputT>> FnT.parDo():
        ParDo.SingleOutput<InputT, OutputT> = ParDo.of(this)

/**
 * create singleton view
 */
fun <T> PCollection<T>.singleton(): PCollectionView<T> =
    this.apply(View.asSingleton())

/**
 * clear state and print afterClearMessage
 * @param afterClearMessage message for print after clear
 */
fun State.clear(afterClearMessage: String) {
    this.clear().apply { println(afterClearMessage) }
}

/**
 * Add Repeatedly trigger
 */
fun Trigger.repeatedlyForever() =
    Repeatedly.forever(this)

infix fun <T> T.tv(instant: Instant): TimestampedValue<T> =
    TimestampedValue.of(this, instant)

/**
 * An inline extension function for FlatMapElements that takes a lambda function as a parameter.
 * The function transforms an input of type InputT to an Iterable of type OutputT.
 *
 * @param fn A lambda function to transform input to an Iterable of output elements.
 * @return A FlatMapElements instance with the applied transformation.
 */
inline fun <InputT, OutputT> FlatMapElements<*, OutputT>.kVia(
    crossinline fn: (InputT) -> Iterable<OutputT>,
): FlatMapElements<InputT, OutputT> =
    ProcessFunction<InputT, Iterable<OutputT>> { fn(it) }.let { this.via(it) }

/**
 * An inline extension function for MapElements that takes a lambda function as a parameter.
 * The function transforms an input of type InputT to an output of type OutputT.
 *
 * @param fn A lambda function to transform the input to an output element.
 * @return A MapElements instance with the applied transformation.
 */
inline fun <InputT, OutputT> MapElements<*, OutputT>.kVia(
    crossinline fn: (InputT) -> OutputT,
): MapElements<InputT, OutputT> =
    ProcessFunction<InputT, OutputT> { fn(it) }
        .let { this.via(it) }
