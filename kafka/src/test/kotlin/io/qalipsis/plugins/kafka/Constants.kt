/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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