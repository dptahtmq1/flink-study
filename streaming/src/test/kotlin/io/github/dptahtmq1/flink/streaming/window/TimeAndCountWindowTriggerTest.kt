package io.github.dptahtmq1.flink.streaming.window

import io.github.dptahtmq1.flink.streaming.functions.BatchKeySelector
import io.github.dptahtmq1.flink.streaming.functions.PassRecordProcessWindowFunction
import io.github.dptahtmq1.flink.streaming.model.User
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Instant
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals


class TimeAndCountWindowTriggerTest {
    private val collectSink = CollectSink()

    @Test
    fun `should fire window by time or count condition`() {
        // Given
        val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
        val trigger = TimeAndCountWindowTrigger<User>(2)

        val record1 = User("test1", 10, Instant.ofEpochMilli(1001))
        val record2 = User("test2", 15, Instant.ofEpochMilli(1002))
        val record3 = User("test3", 15, Instant.ofEpochMilli(1003))

        // When
        environment.fromCollection(listOf(record1, record2, record3))
            .keyBy(BatchKeySelector(1))
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5000L)))
            .trigger(trigger)
            .process(PassRecordProcessWindowFunction())
            .addSink(collectSink)

        environment.execute()

        // Then
        assertEquals(2, collectSink.getValues().size)
        assertEquals(2, collectSink.getValues()[0].size)
        assertEquals(1, collectSink.getValues()[1].size)
    }

    @AfterTest
    fun teardown() {
        collectSink.clear()
    }

    class CollectSink : SinkFunction<List<User>> {
        companion object {
            private val values = mutableListOf<List<User>>()
            private val readWriteLock: ReadWriteLock = ReentrantReadWriteLock()
        }

        override fun invoke(value: List<User>, context: SinkFunction.Context) {
            readWriteLock.writeLock().lock()
            try {
                values.add(value)
            } finally {
                readWriteLock.writeLock().unlock()
            }
        }

        fun getValues(): List<List<User>> {
            readWriteLock.readLock().lock()
            try {
                return Collections.unmodifiableList(values)
            } finally {
                readWriteLock.readLock().unlock()
            }
        }

        fun clear() {
            values.clear()
        }
    }
}
