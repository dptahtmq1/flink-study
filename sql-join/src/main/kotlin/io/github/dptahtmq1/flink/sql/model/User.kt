package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable
import java.time.Instant

data class User(
    var id: Int = 0,
    var name: String = "",
    var createTime: Instant = Instant.ofEpochMilli(1001)
) : Serializable {
    constructor() : this(0, "", Instant.ofEpochMilli(1001))
}
