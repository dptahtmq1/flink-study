package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable

data class Order(
    var id: Int = 0,
    var userId: Int = 0,
    var amount: Long = 0,
    var eventTime: Long = System.currentTimeMillis()
) : Serializable {
    constructor() : this(0, 0, 0, 0)
}
