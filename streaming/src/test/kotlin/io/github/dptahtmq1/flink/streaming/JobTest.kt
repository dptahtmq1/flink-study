package io.github.dptahtmq1.flink.streaming

import kafka.server.KafkaServerStartable
import org.apache.curator.test.TestingServer
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import kotlin.test.AfterTest
import kotlin.test.BeforeTest

abstract class JobTest {
    companion object {
        val flinkCluster = MiniClusterWithClientResource(
            MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(1)
                .setNumberTaskManagers(1)
                .build()
        )
    }

    private lateinit var zkTestServer: TestingServer

    private lateinit var kafkaServer: KafkaServerStartable

    @BeforeTest
    fun setupJob() {
        zkTestServer = TestStreamUtils.startZookeeper()
        kafkaServer = TestStreamUtils.startKafka()
    }

    @AfterTest
    fun teardownJob() {
        TestStreamUtils.stopKafka()
        TestStreamUtils.stopZookeeper()
    }
}
