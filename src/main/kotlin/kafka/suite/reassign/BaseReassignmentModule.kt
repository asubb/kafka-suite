package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.*

abstract class BaseReassignmentModule : RunnableModule {

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {

        val strategy = getStrategy(cli, kafkaAdminClient)
        val newPlan = strategy.newPlan()

        println("New assigment plan: $newPlan")

        if (!dryRun) {
            // TODO check if there is an assignment in progress

            if (!kafkaAdminClient.reassignPartitions(newPlan)) {
                println("ERROR: Can't reassign partitions")
                return
            }

            if (waitToFinish) {
                val start = System.currentTimeMillis()
                while (!kafkaAdminClient.isReassignmentFinished(newPlan)) {
                    println("Elapsed ${(System.currentTimeMillis() - start) / 1000.0f}s")
                    Thread.sleep(10000)
                }
            }
        }
    }

    abstract fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient): PartitionAssignmentStrategy
}