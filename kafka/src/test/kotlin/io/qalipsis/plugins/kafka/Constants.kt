/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.kafka

import kotlin.math.pow

/**
 *
 * @author Eric Jessé
 */
internal object Constants {

    const val DOCKER_IMAGE = "confluentinc/cp-kafka:7.6.0"
    val DOCKER_MAX_MEMORY = 512 * 1024.0.pow(2).toLong()
    const val DOCKER_CPU_COUNT = 2
    const val KAFKA_HEAP_OPTS_ENV = "KAFKA_HEAP_OPTS"
    const val KAFKA_HEAP_OPTS_ENV_VALUE = "-Xms256m -Xmx256m"
}