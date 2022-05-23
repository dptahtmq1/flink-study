package io.github.dptahtmq1.flink.streaming.functions

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class PassRecordProcessWindowFunction<T> :
    ProcessWindowFunction<T, List<T>, Int, TimeWindow>() {
    override fun process(
        key: Int,
        context: Context,
        elements: MutableIterable<T>,
        out: Collector<List<T>>
    ) {
        out.collect(elements.toList())
    }
}
