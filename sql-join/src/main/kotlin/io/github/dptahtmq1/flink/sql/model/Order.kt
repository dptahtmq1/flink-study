package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable
import java.time.Instant

data class Order(
    var id: Int = 0,
    var userId: Int = 0,
    var amount: Long = 0,
    var eventTime: Instant = Instant.ofEpochMilli(1001)
) : Serializable {
    constructor() : this(0, 0, 0, Instant.ofEpochMilli(1001))
}
