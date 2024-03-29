package io.github.dptahtmq1.flink.sql

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource

abstract class JobTest {
    companion object {
        val flinkCluster = MiniClusterWithClientResource(
            MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(1)
                .setNumberTaskManagers(1)
                .build()
        )
    }
}
