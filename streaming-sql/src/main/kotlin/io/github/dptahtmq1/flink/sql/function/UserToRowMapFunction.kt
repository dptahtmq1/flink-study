package io.github.dptahtmq1.flink.sql.function

import io.github.dptahtmq1.flink.sql.common.EventType
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind

class UserToRowMapFunction : MapFunction<User, Row> {
    override fun map(user: User): Row {
        if (user.eventType == EventType.DELETE) {
            return Row.ofKind(RowKind.DELETE, user.id, user.name, user.createTime)
        }
        return Row.ofKind(RowKind.INSERT, user.id, user.name, user.createTime)
    }
}
