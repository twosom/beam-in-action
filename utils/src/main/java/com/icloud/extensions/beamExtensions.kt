package com.icloud.extensions

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.state.State
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView

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