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

package io.qalipsis.plugins.kafka.consumer

import io.qalipsis.api.meters.Counter

/**
 * Records the metrics for the Kafka producer.
 *
 * @property consumedKeyBytesCounter records the number of bytes consumed for the serialized keys.
 * @property consumedValueBytesCounter records the number of bytes consumed for the serialized values.
 * @property consumedRecordsCounter counts the number of records consumed.
 *
 * @author Alex Averyanov
 */
internal class KafkaConsumerMetrics(
    private var consumedKeyBytesCounter: Counter? = null,
    private var consumedValueBytesCounter: Counter? = null,
    private var consumedRecordsCounter: Counter? = null
) {

    fun countKeyBytes(size: Int) {
        consumedKeyBytesCounter?.increment(size.coerceAtLeast(0).toDouble())
    }

    fun countValueBytes(size: Int) {
        consumedValueBytesCounter?.increment(size.coerceAtLeast(0).toDouble())
    }

    fun countRecords() {
        consumedRecordsCounter?.increment()
    }
}