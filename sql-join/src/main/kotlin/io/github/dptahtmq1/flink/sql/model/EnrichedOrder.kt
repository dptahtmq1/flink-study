package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable
import java.time.Instant

data class EnrichedOrder(
    var userName: String,
    var amount: Long,
    var eventTime: Instant
) : Serializable {
    constructor() : this("", 0, Instant.ofEpochMilli(1001))
}
