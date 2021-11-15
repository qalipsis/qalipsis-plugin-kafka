package io.qalipsis.plugins.kafka

import kotlin.math.pow

/**
 *
 * @author Eric Jessé
 */
internal object Constants {

    const val DOCKER_IMAGE = "confluentinc/cp-kafka:5.5.2"
    val DOCKER_MAX_MEMORY = 512 * 1024.0.pow(2).toLong()
    const val DOCKER_CPU_COUNT = 2
    const val KAFKA_HEAP_OPTS_ENV = "KAFKA_HEAP_OPTS"
    const val KAFKA_HEAP_OPTS_ENV_VALUE = "-Xms256m -Xmx256m"
}