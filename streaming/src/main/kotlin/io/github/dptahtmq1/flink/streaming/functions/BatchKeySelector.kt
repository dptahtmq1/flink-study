package io.github.dptahtmq1.flink.streaming.functions

import io.github.dptahtmq1.flink.streaming.model.User
import org.apache.flink.api.java.functions.KeySelector

class BatchKeySelector(
    private val windowCount: Int
) : KeySelector<User, Int> {
    override fun getKey(value: User): Int {
        return value.score.mod(windowCount)
    }
}
