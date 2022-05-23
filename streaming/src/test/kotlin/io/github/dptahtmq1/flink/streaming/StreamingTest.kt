package io.github.dptahtmq1.flink.streaming

import io.github.dptahtmq1.flink.streaming.model.User
import io.github.dptahtmq1.flink.streaming.schema.UserKafkaDeserializationSchema
import org.apache.commons.lang3.SerializationUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.test.AfterTest
import kotlin.test.Test

class StreamingTest : JobTest() {
    private val collectSink = CollectSink()
    private val topic = "topic1"

    @Test
    fun `should aggregate and sink`() {
        // Given
        val windowCount = 2
        val batchIntervalMs = 1000L
        val batchCount = 100L

        val record1 = User("test1", 10, Instant.ofEpochMilli(1001))
        val record2 = User("test2", 15, Instant.ofEpochMilli(1002))
        val record3 = User("test3", 15, Instant.ofEpochMilli(1003))

        val app = Streaming(windowCount, batchIntervalMs, batchCount)

        // Create kafka source
        val kafkaSource = createKafkaSource()
        val sourceStream = app.environment.addSource(kafkaSource)

        // Create kafka producer and send record
        val kafkaProducer = TestStreamUtils.createKafkaProducer<ByteArray, ByteArray>()
        kafkaProducer.send(ProducerRecord(topic, record1.name.toByteArray(), SerializationUtils.serialize(record1)))
        kafkaProducer.send(ProducerRecord(topic, record2.name.toByteArray(), SerializationUtils.serialize(record2)))
        kafkaProducer.send(ProducerRecord(topic, record3.name.toByteArray(), SerializationUtils.serialize(record3)))

        // When
        app.build(sourceStream, collectSink)
        app.environment.parallelism = 1
        app.environment.executeDuringDuration(Duration.ofMillis(5000L))

        // Then
        val result = collectSink.getValues()
        assertEquals(2, result.size)
    }

    @Test
    fun `should aggregate and sink by batch count`() {
        // Given
        val windowCount = 1
        val batchIntervalMs = 1000L
        val batchCount = 2L

        val record1 = User("test1", 10, Instant.ofEpochMilli(1001))
        val record2 = User("test2", 15, Instant.ofEpochMilli(1002))
        val record3 = User("test3", 15, Instant.ofEpochMilli(1003))

        val app = Streaming(windowCount, batchIntervalMs, batchCount)

        // Create kafka source
        val kafkaSource = createKafkaSource()
        val sourceStream = app.environment.addSource(kafkaSource)

        // Create kafka producer and send record
        val kafkaProducer = TestStreamUtils.createKafkaProducer<ByteArray, ByteArray>()
        kafkaProducer.send(ProducerRecord(topic, record1.name.toByteArray(), SerializationUtils.serialize(record1)))
        kafkaProducer.send(ProducerRecord(topic, record2.name.toByteArray(), SerializationUtils.serialize(record2)))
        kafkaProducer.send(ProducerRecord(topic, record3.name.toByteArray(), SerializationUtils.serialize(record3)))

        // When
        app.build(sourceStream, collectSink)
        app.environment.parallelism = 1
        app.environment.executeDuringDuration(Duration.ofMillis(5000L))

        // Then
        val result = collectSink.getValues()
        assertEquals(2, result.size)
        assertEquals(2, result[0].size)
        assertEquals(1, result[1].size)
    }

    private fun createKafkaSource(): SourceFunction<User> {
        val properties = TestStreamUtils.createKafkaConsumerProperties("test-group")
        val deserializationSchema = UserKafkaDeserializationSchema()
        return FlinkKafkaConsumer(topic, deserializationSchema, properties)
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
