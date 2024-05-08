package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable

data class EnrichedOrder(
    var orderId: Int,
    var userName: String,
    var amount: Long,
    var eventTime: Long
) : Serializable {
    constructor() : this(0, "", 0, 0)
}
