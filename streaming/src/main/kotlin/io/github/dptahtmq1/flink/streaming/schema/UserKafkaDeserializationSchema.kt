package io.github.dptahtmq1.flink.streaming.schema

import io.github.dptahtmq1.flink.streaming.model.User
import org.apache.commons.lang3.SerializationUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class UserKafkaDeserializationSchema : KafkaDeserializationSchema<User> {
    override fun getProducedType(): TypeInformation<User> {
        return TypeInformation.of(User::class.java)
    }

    override fun isEndOfStream(nextElement: User?): Boolean {
        return false
    }

    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): User {
        return SerializationUtils.deserialize(record.value())
    }
}
