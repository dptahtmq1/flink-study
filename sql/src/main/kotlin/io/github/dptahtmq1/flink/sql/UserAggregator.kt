package io.github.dptahtmq1.flink.sql

import io.github.dptahtmq1.flink.sql.model.AggregatedUser
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

class UserAggregator(
    private val windowDurationSecond: Long
) {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    private val tableEnvironment = StreamTableEnvironment.create(environment)

    fun build(
        sourceStream: SingleOutputStreamOperator<User>,
        sinkFunction: SinkFunction<AggregatedUser>
    ) {
        // Create source table
        val inputTable = tableEnvironment.fromDataStream(
            sourceStream,
            Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(eventTime AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime - INTERVAL '$windowDurationSecond' SECOND")
                .build()
        )

        // Create view
        tableEnvironment.createTemporaryView("InputTable", inputTable)

        // Create continuous query and result table
        val resultTable = tableEnvironment.sqlQuery(
            """
                SELECT TUMBLE_START(rowtime, INTERVAL '$windowDurationSecond' SECONDS) AS `start`, SUM(score) AS `sum`
                FROM InputTable
                GROUP BY TUMBLE(rowtime, INTERVAL '$windowDurationSecond' SECONDS)
            """.trimIndent()
        )

        // Convert table to stream
        tableEnvironment.toDataStream(resultTable, AggregatedUser::class.java)
            .addSink(sinkFunction)
    }

}
