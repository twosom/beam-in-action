package com.icloud.extensions

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.io.kafka.TimestampPolicy
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory
import org.apache.beam.sdk.state.State
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Trigger
import org.apache.beam.sdk.values.*
import org.apache.kafka.common.TopicPartition
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.reflect.KClass

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
fun <InputT> PCollection<InputT>.singletonView(): PCollectionView<InputT> =
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
fun <TriggerT : Trigger> TriggerT.repeatedlyForever(): Repeatedly =
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
    ProcessFunction<InputT, OutputT> { fn(it) }.let { this.via(it) }

infix fun <KeyT, ValueT> TypeDescriptor<KeyT>.kvs(
    valueTypeDescriptor: TypeDescriptor<ValueT>,
): TypeDescriptor<KV<KeyT, ValueT>> = TypeDescriptors.kvs(this, valueTypeDescriptor)

/**
 * for kotlin
 */
abstract class KTimestampPolicyFactory<KeyT, ValueT> :
    TimestampPolicyFactory<KeyT, ValueT> {

    abstract fun createTimestampPolicy(
        tp: TopicPartition,
        previousWatermark: Instant?,
    ): TimestampPolicy<KeyT, ValueT>

    //DO NOT USE THIS
    override fun createTimestampPolicy(
        tp: TopicPartition,
        previousWatermark: Optional<Instant>,
    ): TimestampPolicy<KeyT, ValueT> =
        createTimestampPolicy(tp, previousWatermark.orElse(null))
}


/**
 * kotlin nullable value to java optional value
 */
fun <T> T?.optional() =
    Optional.ofNullable(this)


fun <T : Number> T.seconds(): Duration =
    Duration.standardSeconds(this.toLong())

fun <T : Number> T.minutes(): Duration =
    Duration.standardMinutes(this.toLong())

fun <T : Number> T.hours(): Duration =
    Duration.standardHours(this.toLong())

/**
 * Beam Type Descriptor sugar method
 */
fun <ClassT : Any> KClass<ClassT>.typeDescriptor(): TypeDescriptor<ClassT> =
    TypeDescriptor.of(this.java)

fun <T> T.timestampedValue(timestamp: Instant): TimestampedValue<T> =
    TimestampedValue.of(this, timestamp)


val <InputT> PCollection<InputT>.typedWindowingStrategy: WindowingStrategy<InputT, BoundedWindow>
    @Suppress("UNCHECKED_CAST")
    get() = this.windowingStrategy as WindowingStrategy<InputT, BoundedWindow>

/**
 * create KVCoder<KeyT, ValueT>
 */
infix fun <KeyT, ValueT> Coder<KeyT>.kvc(valueCoder: Coder<ValueT>): KvCoder<KeyT, ValueT> =
    KvCoder.of(this, valueCoder)

operator fun <KeyT, ValueT> KV<KeyT, ValueT>.component1(): KeyT = this.key

operator fun <KeyT, ValueT> KV<KeyT, ValueT>.component2(): ValueT = this.value


typealias KPredicate<InputT> = (InputT) -> Boolean

/**
 * [Filter] class for kotlin language
 */
object KFilter {

    /**
     * @see Filter.by
     */
    inline fun <InputT> by(
        crossinline fn: KPredicate<InputT>,
    ): Filter<InputT> =
        Filter.by(ProcessFunction { fn(it) })
}

fun <ClassT : Any> KClass<ClassT>.logger(): Logger = LoggerFactory.getLogger(this.java)
