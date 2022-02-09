package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable
import java.time.Instant

data class AggregatedUser(
    var start: Instant = Instant.ofEpochMilli(1001),
    var sum: Int = 0
) : Serializable {
    constructor() : this(Instant.ofEpochMilli(1001), 0)
}
