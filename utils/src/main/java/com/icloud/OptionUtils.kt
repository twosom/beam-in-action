package com.icloud

import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.common.ReflectHelpers


object OptionUtils {

    @JvmStatic
    @Suppress("UNCHECKED_CAST")
    fun <T : PipelineOptions> createOption(
        args: Array<String>,
        optionClass: Class<T>?,
    ): T {
        (optionClass ?: return PipelineOptionsFactory.fromArgs(*args).withValidation()
            .create()
            .let { withApplicationName(it as T) } as T)
            .apply { PipelineOptionsFactory.register(optionClass) }

        return PipelineOptionsFactory.fromArgs(*args)
            .withValidation().let {
                withApplicationName(it.`as`(optionClass)) as T
            }
    }


    private fun withApplicationName(
        options: PipelineOptions,
    ): PipelineOptions = findCallerClassName().let {
        options.`as`(ApplicationNameOptions::class.java).appName = it
        options
    }

    private fun findCallerClassName(): String =
        Thread.currentThread().stackTrace.last().let {
            try {
                Class.forName(it.className, true, ReflectHelpers.findClassLoader())
                    .simpleName
            } catch (e: ClassCastException) {
                "unknown"
            }
        }
}
