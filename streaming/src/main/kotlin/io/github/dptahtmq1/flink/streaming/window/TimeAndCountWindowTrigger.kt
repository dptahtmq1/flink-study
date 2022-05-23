package io.github.dptahtmq1.flink.streaming.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TimeAndCountWindowTrigger<T>(
    private val maxCount: Long
) : Trigger<T, TimeWindow>() {
    private val stateDesc = ReducingStateDescriptor("count", Sum(), LongSerializer.INSTANCE)

    override fun onElement(
        element: T,
        timestamp: Long,
        window: TimeWindow,
        ctx: TriggerContext
    ): TriggerResult {
        val count = ctx.getPartitionedState(stateDesc)
        count.add(1L)
        return if (count.get() >= maxCount || window.maxTimestamp() <= ctx.currentWatermark) {
            count.clear()
            TriggerResult.FIRE_AND_PURGE
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp())
            TriggerResult.CONTINUE
        }
    }

    override fun onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        return if (time >= window.end) {
            TriggerResult.CONTINUE
        } else {
            TriggerResult.FIRE_AND_PURGE
        }
    }

    override fun onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult {
        return if (time >= window.end) {
            TriggerResult.CONTINUE
        } else {
            TriggerResult.FIRE_AND_PURGE
        }
    }

    override fun clear(window: TimeWindow, ctx: TriggerContext) {
        ctx.getPartitionedState(stateDesc).clear()
        ctx.deleteEventTimeTimer(window.maxTimestamp())
    }

    override fun canMerge(): Boolean = false

    private class Sum : ReduceFunction<Long> {
        @Throws(Exception::class)
        override fun reduce(value1: Long, value2: Long): Long {
            return value1 + value2
        }

        companion object {
            private const val serialVersionUID = -6614L
        }
    }
}
