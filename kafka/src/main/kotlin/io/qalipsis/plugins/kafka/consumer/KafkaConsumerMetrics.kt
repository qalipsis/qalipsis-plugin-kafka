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