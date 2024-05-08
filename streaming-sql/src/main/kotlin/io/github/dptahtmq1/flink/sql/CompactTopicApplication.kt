package io.github.dptahtmq1.flink.sql

import io.github.dptahtmq1.flink.sql.model.EnrichedOrder
import io.github.dptahtmq1.flink.sql.model.Order
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

class CompactTopicApplication {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    private val tableEnvironment = StreamTableEnvironment.create(environment)

    init {
        val configuration = tableEnvironment.config.configuration
        configuration.setString("table.exec.source.idle-timeout", "3 s")
    }

    fun compactTopicOnly(
        userStream: SingleOutputStreamOperator<User>,
        sinkFunction: SinkFunction<User>
    ) {
        val userTable = tableEnvironment.fromDataStream(
            userStream,
            Schema.newBuilder().build()
        )

        tableEnvironment.createTemporaryView("UserTable", userTable)

        val userQueryResult = tableEnvironment.sqlQuery(
            """
                SELECT *
                FROM UserTable
            """.trimIndent()
        )

        tableEnvironment.toDataStream(userQueryResult, User::class.java)
            .addSink(sinkFunction)
    }

    fun build(
        userStream: SingleOutputStreamOperator<User>,
        orderStream: SingleOutputStreamOperator<Order>,
        sinkFunction: SinkFunction<EnrichedOrder>
    ) {
        // Create user table
        val userTable = tableEnvironment.fromDataStream(
            userStream,
            Schema.newBuilder()
                .primaryKey("id")
                .columnByExpression("rowTime", "TO_TIMESTAMP_LTZ(createTime, 3)")
                .watermark("rowTime", "rowTime - INTERVAL '2' SECOND")
                .build()
        )

        // Create view
        tableEnvironment.createTemporaryView("UserTable", userTable)

        val orderTable = tableEnvironment.fromDataStream(
            orderStream,
            Schema.newBuilder()
                .primaryKey("id")
                .columnByExpression("proctime", "TO_TIMESTAMP_LTZ(eventTime, 3)")
                .watermark("proctime", "proctime - INTERVAL '2' SECOND")
                .build()
        )

        tableEnvironment.createTemporaryView("OrderTable", orderTable)

        orderTable.select()

        // Create joined table
        val joinedTable = tableEnvironment.sqlQuery(
            """
                SELECT O.id AS orderId, U.name AS userName, O.amount, O.eventTime
                FROM OrderTable O
                JOIN UserTable FOR SYSTEM_TIME AS OF O.proctime AS U
                ON U.id = O.userId
            """.trimIndent()
        )

        // Convert table to stream
        tableEnvironment.toDataStream(joinedTable, EnrichedOrder::class.java)
            .addSink(sinkFunction)
    }
}
