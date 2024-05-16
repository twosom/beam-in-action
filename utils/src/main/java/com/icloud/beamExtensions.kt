package com.icloud

import org.apache.beam.sdk.values.KV

/**
 * infix method for create KV...
 */
infix fun <K, V> K.kv(v: V): KV<K, V> = KV.of(this, v)