package io.github.dptahtmq1.flink.sql

import io.github.dptahtmq1.flink.sql.model.AggregatedUser
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.time.Instant
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

class UserAggregatorTest : JobTest() {

    private val collectSink = CollectSink()

    @Test
    fun `should aggregate to one time window`() {
        // Given
        val windowDurationSecond = 2L
        val record1 = User("test1", 10, Instant.ofEpochMilli(1001))
        val record2 = User("test2", 15, Instant.ofEpochMilli(1002))
        val record3 = User("test3", 15, Instant.ofEpochMilli(1003))

        // When
        val app = UserAggregator(windowDurationSecond)
        val source = app.environment.fromElements(record1, record2, record3)
        app.build(source, collectSink)
        app.environment.execute()

        // Then
        val result = collectSink.getValues()
        assertEquals(1, result.size)
        assertEquals(40, result[0].sum)
        assertEquals(Instant.ofEpochMilli(0), result[0].start)
    }

    @Test
    fun `should aggregate by user's eventTime`() {
        // Given
        val windowDurationSecond = 2L
        val record1 = User("test1", 10, Instant.ofEpochMilli(1000))
        val record2 = User("test2", 15, Instant.ofEpochMilli(2001))
        val record3 = User("test3", 15, Instant.ofEpochMilli(3001))

        // When
        val app = UserAggregator(windowDurationSecond)
        val source = app.environment.fromElements(record1, record2, record3)
        app.build(source, collectSink)
        app.environment.execute()

        // Then
        val result = collectSink.getValues()
        assertEquals(2, result.size)

        assertEquals(10, result[0].sum)
        assertEquals(Instant.ofEpochMilli(0), result[0].start)

        assertEquals(30, result[1].sum)
        assertEquals(Instant.ofEpochMilli(2000), result[1].start)
    }

    @AfterTest
    fun teardown() {
        collectSink.clear()
    }

    class CollectSink : SinkFunction<AggregatedUser> {
        companion object {
            private val values = mutableListOf<AggregatedUser>()
            private val readWriteLock: ReadWriteLock = ReentrantReadWriteLock()
        }

        override fun invoke(value: AggregatedUser, context: SinkFunction.Context) {
            readWriteLock.writeLock().lock()
            try {
                values.add(value)
            } finally {
                readWriteLock.writeLock().unlock()
            }
        }

        fun getValues(): List<AggregatedUser> {
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
