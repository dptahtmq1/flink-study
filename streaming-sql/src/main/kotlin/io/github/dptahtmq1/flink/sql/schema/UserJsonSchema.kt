package io.github.dptahtmq1.flink.sql.schema

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.dptahtmq1.flink.sql.common.logger
import io.github.dptahtmq1.flink.sql.model.User
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

class UserJsonSchema : KafkaRecordDeserializationSchema<User> {
    private val log = logger()
    private val objectMapper = ObjectMapper()

    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>, out: Collector<User>) {
        try {
            val value = objectMapper.readValue(record.value(), User::class.java)
            out.collect(value)
        } catch (e: Exception) {
            log.error("failed to parse record value, {}", e.message)
            null
        }
    }

    override fun getProducedType(): TypeInformation<User> {
        return TypeInformation.of(User::class.java)
    }
}
