package com.icloud

import org.apache.beam.sdk.coders.VoidCoder
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class WithReadDelay<Type>(
    private val delay: Duration,
) :
    PTransform<PCollection<Type>, PCollection<Type>>() {
    override fun expand(input: PCollection<Type>): PCollection<Type> {
        return input.apply(WithKeys.of(""))
            .apply(ParDo.of(DelayFn(this.delay)))
            .apply(Values.create())
    }

    class DelayFn<T>(private val delay: Duration) : DoFn<T, T>() {

        companion object {
            private val LOG: Logger = LoggerFactory.getLogger(DelayFn::class.java)
            fun <T> of(delay: Duration): DelayFn<T> {
                return DelayFn(delay)
            }
        }

        @StateId("dummy")
        private val state = StateSpecs.value(VoidCoder.of())

        @ProcessElement
        fun process(
            @Element element: T,
            output: OutputReceiver<T>,
        ) {
            try {
                TimeUnit.MILLISECONDS.sleep(delay.millis)
            } catch (e: InterruptedException) {
                LOG.error("[DelayFn] occurred errors with ", e)
                Thread.currentThread().interrupt()
            }
            output.output(element)
        }
    }

    companion object {
        @JvmStatic
        fun <Type> ofProcessingTime(delay: Duration): WithReadDelay<Type> {
            return WithReadDelay(delay)
        }
    }
}
