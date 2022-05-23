package io.github.dptahtmq1.flink.streaming.functions

import io.mockk.mockk
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import java.util.*
import java.util.stream.Collectors
import java.util.stream.StreamSupport
import kotlin.test.Test
import kotlin.test.assertEquals

class PassRecordProcessWindowFunctionTest {

    @Test
    fun `should process windowed values`() {
        // Given
        val processFunction = PassRecordProcessWindowFunction<String>()
        val context = mockk<ProcessWindowFunction<String, List<String>, Int, TimeWindow>.Context>()

        val recordList = listOf("test1", "test2", "test3", "test4")

        val valueIterator = recordList.iterator()
        val elements = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(valueIterator, 0), false)
            .collect(Collectors.toList())

        val outputList = mutableListOf<List<String>>()
        val out = ListCollector(outputList)

        // When
        processFunction.process(1, context, elements, out)

        // Then
        assertEquals(1, outputList.size)
        assertEquals(recordList.size, outputList[0].size)
    }
}
