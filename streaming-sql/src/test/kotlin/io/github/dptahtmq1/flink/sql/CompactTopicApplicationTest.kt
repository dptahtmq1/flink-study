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
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
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
        send(kafkaProducer, userTopic, user1)
        send(kafkaProducer, userTopic, user2)
        send(kafkaProducer, userTopic, user3)

        val order1 = Order(1, 1, 100, current)
        val order2 = Order(2, 1, 200, current + 1500)
        val order3 = Order(3, 1, 300, current + 5000)

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
        val sink = CollectSink()
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
                "KafkaBroker" to kafka.bootstrapServers,
                "KafkaSourceBounded" to "true"
            )
        )

        val kafkaProducer = createKafkaProducer<ByteArray, ByteArray>(parameterTool)

        val current = System.currentTimeMillis()
        val user1 = User(1, "test1", current)
        val user2 = User(2, "test2", current)
        val user3 = User(3, "test3", current + 1000)
        send(kafkaProducer, userTopic, user1)
        send(kafkaProducer, userTopic, user2)
        send(kafkaProducer, userTopic, user3)

        val changedUser1 = User(1, "test1-changed", current + 5000)
        send(kafkaProducer, userTopic, changedUser1)

        val order1 = Order(1, 1, 100, current)
        val order2 = Order(2, 1, 200, current + 1500)
        val order3 = Order(3, 1, 300, current + 5000)  // should be stored
        val order4 = Order(4, 4, 300, current + 5000)  // should be dropped
        send(kafkaProducer, orderTopic, order1)
        send(kafkaProducer, orderTopic, order2)
        send(kafkaProducer, orderTopic, order3)
        send(kafkaProducer, orderTopic, order4)

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

        val sink = CollectSink()
        app.build(userSource, orderSource, sink)
        app.environment.execute()

        // Then
        println(sink.getValues())
        assertEquals(3, sink.getValues().size)
    }

    @AfterTest
    fun teardown() {
        val client = AdminClient.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers
            )
        )
        client.deleteTopics(listOf(userTopic, orderTopic))

        kafka.stop()
    }

    private fun send(kafkaProducer: KafkaProducer<ByteArray, ByteArray>, topic: String, user: User) {
        kafkaProducer.send(ProducerRecord(topic, user.name.toByteArray(), objectMapper.writeValueAsBytes(user)))
            .get()
    }

    private fun send(kafkaProducer: KafkaProducer<ByteArray, ByteArray>, topic: String, order: Order) {
        kafkaProducer.send(
            ProducerRecord(
                topic,
                order.id.toString().toByteArray(),
                objectMapper.writeValueAsBytes(order)
            )
        ).get()
    }

    private fun sendUserDelete(kafkaProducer: KafkaProducer<ByteArray, ByteArray>, topic: String, user: User) {
        kafkaProducer.send(ProducerRecord(topic, user.name.toByteArray(), null)).get()
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

        val builder = KafkaSource.builder<T>()
            .setProperties(properties)
            .setDeserializer(schema)
            .setTopics(topic)

        if (params.getBoolean("KafkaSourceBounded", false)) {
            builder.setBounded(OffsetsInitializer.latest())
        }

        return builder.build()
    }

    private fun <K, V> createKafkaProducer(params: ParameterTool): KafkaProducer<K, V> {
        val properties = Properties().also {
            it.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params["KafkaBroker"])
            it.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER_CLASS)
            it.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER_CLASS)
        }

        return KafkaProducer<K, V>(properties)
    }

    class CollectSink : SinkFunction<EnrichedOrder> {
        companion object {
            private val values = mutableListOf<Any>()
            private val readWriteLock: ReadWriteLock = ReentrantReadWriteLock()
        }

        override fun invoke(value: EnrichedOrder, context: SinkFunction.Context) {
            readWriteLock.writeLock().lock()
            try {
                println("$value, elapsed: ${System.currentTimeMillis() - value.eventTime}")
                values.add(value)
            } finally {
                readWriteLock.writeLock().unlock()
            }
        }

        fun getValues(): List<EnrichedOrder> {
            readWriteLock.readLock().lock()
            try {
                return Collections.unmodifiableList(values) as List<EnrichedOrder>
            } finally {
                readWriteLock.readLock().unlock()
            }
        }

        fun clear() {
            values.clear()
        }
    }
}
