package io.github.dptahtmq1.flink.sql.schema

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.dptahtmq1.flink.sql.common.logger
import io.github.dptahtmq1.flink.sql.model.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

class OrderJsonSchema : KafkaRecordDeserializationSchema<Order> {
    private val log = logger()
    private val objectMapper = ObjectMapper()

    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>, out: Collector<Order>) {
        try {
            val value = objectMapper.readValue(record.value(), Order::class.java)
            out.collect(value)
        } catch (e: Exception) {
            log.error("failed to parse record value, {}", e.message)
            null
        }
    }

    override fun getProducedType(): TypeInformation<Order> {
        return TypeInformation.of(Order::class.java)
    }
}
