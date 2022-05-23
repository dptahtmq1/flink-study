package io.github.dptahtmq1.flink.streaming.model

import java.io.Serializable
import java.time.Instant

data class User(
    var name: String = "",
    var score: Int = 0,
    var eventTime: Instant = Instant.ofEpochMilli(1001)
) : Serializable {
    constructor() : this("", 0, Instant.ofEpochMilli(1001))
}
