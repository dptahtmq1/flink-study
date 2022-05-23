package io.github.dptahtmq1.flink.streaming

import io.github.dptahtmq1.flink.streaming.functions.BatchKeySelector
import io.github.dptahtmq1.flink.streaming.functions.PassRecordProcessWindowFunction
import io.github.dptahtmq1.flink.streaming.model.User
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

class Streaming(
    private val windowCount: Int,
    private val batchInterval: Long
) {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    init {
        environment.config.autoWatermarkInterval = 0
    }

    fun build(
        sourceStream: SingleOutputStreamOperator<User>,
        sinkFunction: SinkFunction<List<User>>
    ) {
        // keyed stream -> window aggregation
        sourceStream
            .uid("streaming-example")
            .keyBy(BatchKeySelector(windowCount))
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(batchInterval)))
            .process(PassRecordProcessWindowFunction())
            .addSink(sinkFunction)
            .name("name")
            .uid("uid")
    }
}
