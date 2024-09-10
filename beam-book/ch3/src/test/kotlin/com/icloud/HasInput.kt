package com.icloud

import com.icloud.Utils

abstract class HasInput {

    val input: List<String>
        get() = Utils.getLines(javaClass.classLoader.getResourceAsStream("lorem.txt")!!)
            .also { println("load data from [lorem.txt]") }

}