package io.github.dptahtmq1.flink.sql

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.dptahtmq1.flink.sql.model.EnrichedOrder
import io.github.dptahtmq1.flink.sql.model.Order
import io.github.dptahtmq1.flink.sql.model.User
import io.github.dptahtmq1.flink.sql.schema.OrderJsonSchema
import io.github.dptahtmq1.flink.sql.schema.UserJsonSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.schedule
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class CompactTopicApplicationTest {

    private val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))

    private val userTopic = "dev.dp.compact.${UUID.randomUUID()}"
    private val orderTopic = "dev.dp.order.${UUID.randomUUID()}"
    private val objectMapper = jacksonObjectMapper()

    companion object {
        private val BYTE_ARRAY_SERIALIZER_CLASS: String = ByteArraySerializer::class.java.name
    }

    @BeforeTest
    fun setup() {
        kafka.start()

        val client = AdminClient.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers
            )
        )
        client.createTopics(
            listOf(
                NewTopic(userTopic, 1, 1).configs(
                    mapOf(
                        "cleanup.policy" to "compact"
                    )
                ),
                NewTopic(orderTopic, 1, 1)
            )
        )
    }

    @Test
    fun `should get values from compact topic`() {
        // Given
        val parameterTool = ParameterTool.fromMap(
            mapOf(
                "KafkaTopic" to userTopic,
                "KafkaBroker" to kafka.bootstrapServers
            )
        )

        val current = System.currentTimeMillis()
        val user1 = User(1, "test1", current)
        val user2 = User(2, "test2", current)
        val user3 = User(3, "test3", current + 25000)

        val kafkaProducer = createKafkaProducer<ByteArray, ByteArray>(parameterTool)
        kafkaProducer.send(ProducerRecord(userTopic, user1.name.toByteArray(), objectMapper.writeValueAsBytes(user1)))
            .get()
        kafkaProducer.send(ProducerRecord(userTopic, user2.name.toByteArray(), objectMapper.writeValueAsBytes(user2)))
            .get()
        kafkaProducer.send(ProducerRecord(userTopic, user3.name.toByteArray(), objectMapper.writeValueAsBytes(user3)))
            .get()

        val order1 = Order(1, 1, 100, current)
        val order2 = Order(2, 1, 200, current + 1500)
        val order3 = Order(3, 1, 300, current + 10000)

        // When
        val app = CompactTopicApplication()
        val kafka = createKafkaSource(
            parameterTool,
            userTopic,
            UserJsonSchema()
        )
        val userSource = app.environment.fromSource(
            kafka,
            WatermarkStrategy.noWatermarks(),
            "compact-kafka"
        )
        val orderSource = app.environment.fromElements(order1, order2, order3)
        val sink = CollectSink<EnrichedOrder>()
        app.build(userSource, orderSource, sink)
        app.environment.executeDuringDuration(Duration.ofSeconds(15))

        // Then
        println(sink.getValues())
        assertEquals(3, sink.getValues().size)
    }

    @Test
    fun `should get values from compact topic and orders topic`() {
        // Given
        val parameterTool = ParameterTool.fromMap(
            mapOf(
                "KafkaBroker" to kafka.bootstrapServers
            )
        )

        val current = System.currentTimeMillis()
        val user1 = User(1, "test1", current)
        val user2 = User(2, "test2", current)
        val user3 = User(3, "test3", current + 25000)

        val kafkaProducer = createKafkaProducer<ByteArray, ByteArray>(parameterTool)
        kafkaProducer.send(ProducerRecord(userTopic, user1.name.toByteArray(), objectMapper.writeValueAsBytes(user1)))
            .get()
        kafkaProducer.send(ProducerRecord(userTopic, user2.name.toByteArray(), objectMapper.writeValueAsBytes(user2)))
            .get()
        kafkaProducer.send(ProducerRecord(userTopic, user3.name.toByteArray(), objectMapper.writeValueAsBytes(user3)))
            .get()

        val order1 = Order(1, 1, 100, current)
        val order2 = Order(2, 1, 200, current + 1500)

        kafkaProducer.send(
            ProducerRecord(
                orderTopic,
                order1.id.toString().toByteArray(),
                objectMapper.writeValueAsBytes(order1)
            )
        ).get()
        kafkaProducer.send(
            ProducerRecord(
                orderTopic,
                order2.id.toString().toByteArray(),
                objectMapper.writeValueAsBytes(order2)
            )
        ).get()

        Timer().schedule(5000L) {
            val order3 = Order(3, 1, 300, current + 10000)  // should be stored
            val order4 = Order(4, 4, 300, current + 10000)  // should be dropped
            kafkaProducer.send(
                ProducerRecord(
                    orderTopic,
                    order3.id.toString().toByteArray(),
                    objectMapper.writeValueAsBytes(order3)
                )
            ).get()
            kafkaProducer.send(
                ProducerRecord(
                    orderTopic,
                    order4.id.toString().toByteArray(),
                    objectMapper.writeValueAsBytes(order4)
                )
            ).get()

            val user4 = User(4, "test4", current + 10000)
            kafkaProducer.send(
                ProducerRecord(
                    userTopic,
                    user4.name.toByteArray(),
                    objectMapper.writeValueAsBytes(user4)
                )
            ).get()

            val changedUser1 = User(1, "test1-changed", current + 10000)
            kafkaProducer.send(
                ProducerRecord(
                    userTopic,
                    changedUser1.name.toByteArray(),
                    objectMapper.writeValueAsBytes(changedUser1)
                )
            ).get()

            val order5 = Order(5, 1, 300, current + 10000)  // should be stored with changed user1
            kafkaProducer.send(
                ProducerRecord(
                    orderTopic,
                    order5.id.toString().toByteArray(),
                    objectMapper.writeValueAsBytes(order5)
                )
            ).get()
        }

        // When
        val app = CompactTopicApplication()

        val userKafkaSource = createKafkaSource(
            parameterTool,
            userTopic,
            UserJsonSchema()
        )
        val userSource = app.environment.fromSource(
            userKafkaSource,
            WatermarkStrategy.noWatermarks(),
            "compact-kafka"
        )

        val orderKafkaSource = createKafkaSource(
            parameterTool,
            orderTopic,
            OrderJsonSchema()
        )
        val orderSource = app.environment.fromSource(
            orderKafkaSource,
            WatermarkStrategy.noWatermarks(),
            "order-topic"
        )

        val sink = CollectSink<EnrichedOrder>()
        app.build(userSource, orderSource, sink)
        app.environment.executeDuringDuration(Duration.ofSeconds(15))

        // Then
        println(sink.getValues())
        assertEquals(5, sink.getValues().size)
    }

    @AfterTest
    fun teardown() {
        val client = AdminClient.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers
            )
        )
        client.deleteTopics(listOf(userTopic))

        kafka.stop()
    }

    private fun <T> createKafkaSource(
        params: ParameterTool,
        topic: String,
        schema: KafkaRecordDeserializationSchema<T>
    ): KafkaSource<T> {
        val properties = Properties().also {
            it.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params["KafkaBroker"])
            it.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            it.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, params.get("KafkaInitialOffset", "earliest"))
        }

        return KafkaSource.builder<T>()
            .setProperties(properties)
            .setDeserializer(schema)
            .setTopics(topic)
            .build()
    }

    private fun <K, V> createKafkaProducer(params: ParameterTool): KafkaProducer<K, V> {
        val properties = Properties().also {
            it.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params["KafkaBroker"])
            it.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER_CLASS)
            it.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER_CLASS)
        }

        return KafkaProducer<K, V>(properties)
    }

    class CollectSink<T : Any> : SinkFunction<T> {
        companion object {
            private val values = mutableListOf<Any>()
            private val readWriteLock: ReadWriteLock = ReentrantReadWriteLock()
        }

        override fun invoke(value: T, context: SinkFunction.Context) {
            readWriteLock.writeLock().lock()
            try {
                values.add(value)
            } finally {
                readWriteLock.writeLock().unlock()
            }
        }

        fun getValues(): List<T> {
            readWriteLock.readLock().lock()
            try {
                return Collections.unmodifiableList(values) as List<T>
            } finally {
                readWriteLock.readLock().unlock()
            }
        }

        fun clear() {
            values.clear()
        }
    }
}
