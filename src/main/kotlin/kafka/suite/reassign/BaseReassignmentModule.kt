package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import mu.KotlinLogging
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

abstract class BaseReassignmentModule : RunnableModule {

    private val logger = KotlinLogging.logger {}

    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun getOptions(): Options = Options().of(t, *getOptionList().toTypedArray())

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {
        val limitToTopics = cli.get(t) { it.first().toString().split(",").toSet() } ?: emptySet()
        val plan = kafkaAdminClient.currentAssignment(limitToTopics)
        logger.debug { "currentAssignment=$plan" }
        val oldReplicasByPartitionAndTopic = plan.partitions
                .map { p -> p.partition to p.topic to p.replicas }
                .toMap()


        val strategy = getStrategy(cli, kafkaAdminClient, plan)
        val newPlan = strategy.newPlan()

        logger.debug { "newPlan=$newPlan" }
        println("New assigment plan:")
        val byTopic = newPlan.partitions.groupBy { it.topic }
        byTopic.forEach { (topic, partitions) ->
            println("Topic: $topic")
            partitions.forEach { p ->
                println(String.format(
                        "\t%3s [%s]->[%s]",
                        p.partition,
                        oldReplicasByPartitionAndTopic.getValue(Pair(p.partition, topic)).joinToString(),
                        p.replicas.joinToString()
                ))
            }
        }

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

    protected abstract fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment): PartitionAssignmentStrategy

    protected abstract fun getOptionList(): List<Option>
}