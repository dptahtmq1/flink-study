package io.github.dptahtmq1.flink.sql.model

import io.github.dptahtmq1.flink.sql.common.EventType
import java.io.Serializable

data class User(
    var id: Int = 0,
    var name: String = "",
    var createTime: Long = System.currentTimeMillis(),
    var eventType: EventType = EventType.CREATE
) : Serializable {
    constructor() : this(0, "", 0, EventType.CREATE)
}
