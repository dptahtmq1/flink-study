package io.github.dptahtmq1.flink.sql.model

import java.io.Serializable

data class User(
    var id: Int = 0,
    var name: String = "",
    var createTime: Long = System.currentTimeMillis()
) : Serializable {
    constructor() : this(0, "", 0)
}
