package com.icloud

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.common.ReflectHelpers
import org.apache.hadoop.conf.Configuration


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

    @JvmStatic
    fun <T : PipelineOptions> createHdfsConf(
        hdfs: String,
        args: Array<String>,
        optionClass: Class<T>?,
    ): PipelineOptions {
        val conf = Configuration()
        conf["fs.defaultFS"] = hdfs
        conf["fs.hdfs.impl"] = "org.apache.hadoop.hdfs.DistributedFileSystem"
        val options = this.createOption(args, optionClass)
            .`as`(HadoopFileSystemOptions::class.java)

        options.hdfsConfiguration = listOf(conf)
        return options
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
