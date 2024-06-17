package com.icloud

import com.icloud.OptionUtils.createHdfsConf
import com.icloud.OptionUtils.createOption
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions

object PipelineUtils {

    fun <T : PipelineOptions> from(
        args: Array<String>,
        clazz: Class<T>,
    ): Pair<Pipeline, T> =
        create(args, clazz).let {
            it to it.options.`as`(clazz)
        }

    @JvmStatic
    @JvmOverloads
    fun <T : PipelineOptions> create(
        args: Array<String> = arrayOf(),
        optionClass: Class<T>? = null,
    ): Pipeline =
        createOption(args, optionClass).pipeline()

    @JvmStatic
    fun <T : PipelineOptions> createWithHdfsConf(
        hdfs: String,
        args: Array<String>,
        optionClass: Class<T>?,
    ): Pipeline =
        createPipeline(createHdfsConf(hdfs, args, optionClass))

    private fun createPipeline(
        option: PipelineOptions,
    ): Pipeline = Pipeline.create(option)


    internal fun <T : PipelineOptions> T.pipeline(): Pipeline =
        createPipeline(this)
}
