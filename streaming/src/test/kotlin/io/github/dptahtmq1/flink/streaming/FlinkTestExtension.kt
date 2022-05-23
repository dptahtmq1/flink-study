package io.github.dptahtmq1.flink.streaming

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.core.execution.JobClient
import org.apache.flink.core.execution.JobListener
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

fun StreamExecutionEnvironment.executeDuringDuration(duration: Duration) {
    val latch = CountDownLatch(1)

    this.registerJobListener(object : JobListener {
        override fun onJobSubmitted(jobClient: JobClient?, throwable: Throwable?) {
            latch.countDown()
        }

        override fun onJobExecuted(jobExecutionResult: JobExecutionResult?, throwable: Throwable?) {}
    })

    val that = this
    val thread = object : Thread() {
        override fun run() {
            that.execute()
        }
    }

    thread.start()

    latch.await(duration.toMillis(), TimeUnit.MILLISECONDS)

    Thread.sleep(duration.toMillis())

    thread.interrupt()
}
