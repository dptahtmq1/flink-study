package io.github.dptahtmq1.flink.sql

import io.github.dptahtmq1.flink.sql.model.EnrichedOrder
import io.github.dptahtmq1.flink.sql.model.Order
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

class OrderEnrichment {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    private val tableEnvironment = StreamTableEnvironment.create(environment)

    fun build(
        userStream: SingleOutputStreamOperator<User>,
        orderStream: SingleOutputStreamOperator<Order>,
        sinkFunction: SinkFunction<EnrichedOrder>
    ) {
        // Create user table
        val userTable = tableEnvironment.fromDataStream(
            userStream,
            Schema.newBuilder().build()
        )

        // Create view
        tableEnvironment.createTemporaryView("UserTable", userTable)

        val orderTable = tableEnvironment.fromDataStream(
            orderStream,
            Schema.newBuilder().build()
        )

        tableEnvironment.createTemporaryView("OrderTable", orderTable)

        // Create joined table
        val joinedTable = tableEnvironment.sqlQuery(
            """
                SELECT U.name AS userName, O.amount, O.eventTime
                FROM UserTable U, OrderTable O
                WHERE U.id = O.userId
            """.trimIndent()
        )

        // Convert table to stream
        tableEnvironment.toDataStream(joinedTable, EnrichedOrder::class.java)
            .addSink(sinkFunction)
    }

}
