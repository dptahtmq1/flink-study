package io.github.dptahtmq1.flink.sql

import io.github.dptahtmq1.flink.sql.model.EnrichedOrder
import io.github.dptahtmq1.flink.sql.model.Order
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.time.Instant
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

class OrderEnrichmentTest : JobTest() {

    private val collectSink = CollectSink()

    @Test
    fun `should enrich username`() {
        // Given
        val user1 = User(1, "test1", Instant.ofEpochMilli(1001))
        val user2 = User(2, "test2", Instant.ofEpochMilli(1002))
        val user3 = User(3, "test3", Instant.ofEpochMilli(1003))

        val order1 = Order(1, 1, 100, Instant.ofEpochMilli(1001))
        val order2 = Order(2, 1, 200, Instant.ofEpochMilli(5001))

        // When
        val app = OrderEnrichment()
        val userSource = app.environment.fromElements(user1, user2, user3)
        val orderSource = app.environment.fromElements(order1, order2)
        app.build(userSource, orderSource, collectSink)
        app.environment.execute()

        // Then
        val result = collectSink.getValues()
        assertEquals(2, result.size)
        assertEquals(user1.name, result[0].userName)
        assertEquals(user1.name, result[1].userName)
    }

    @Test
    fun `should drop non-matched order`() {
        // Given
        val user1 = User(1, "test1", Instant.ofEpochMilli(1001))
        val user2 = User(2, "test2", Instant.ofEpochMilli(1002))
        val user3 = User(3, "test3", Instant.ofEpochMilli(1003))

        val order1 = Order(1, 1, 100, Instant.ofEpochMilli(1001))
        val order2 = Order(2, 1, 200, Instant.ofEpochMilli(5001))
        val order3 = Order(2, -1, 200, Instant.ofEpochMilli(6001))

        // When
        val app = OrderEnrichment()
        val userSource = app.environment.fromElements(user1, user2, user3)
        val orderSource = app.environment.fromElements(order1, order2, order3)
        app.build(userSource, orderSource, collectSink)
        app.environment.execute()

        // Then
        val result = collectSink.getValues()
        assertEquals(2, result.size)
        assertEquals(user1.name, result[0].userName)
        assertEquals(user1.name, result[1].userName)
    }

    @AfterTest
    fun teardown() {
        collectSink.clear()
    }

    class CollectSink : SinkFunction<EnrichedOrder> {
        companion object {
            private val values = mutableListOf<EnrichedOrder>()
            private val readWriteLock: ReadWriteLock = ReentrantReadWriteLock()
        }

        override fun invoke(value: EnrichedOrder, context: SinkFunction.Context) {
            readWriteLock.writeLock().lock()
            try {
                values.add(value)
            } finally {
                readWriteLock.writeLock().unlock()
            }
        }

        fun getValues(): List<EnrichedOrder> {
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
