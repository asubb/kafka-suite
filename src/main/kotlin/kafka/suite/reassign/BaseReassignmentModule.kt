package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import mu.KotlinLogging
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.lang.IllegalStateException

abstract class BaseReassignmentModule : RunnableModule {

    private val logger = KotlinLogging.logger {}

    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")
    private val w = Option("w", "weightFn", true, "The name of the weight function: " + WeightFns.values().joinToString { it.id } + ".")

    override fun getOptions(): Options = Options().of(t, *getOptionList().toTypedArray())

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        val limitToTopics = cli.get(t) { it.split(",").toSet() } ?: emptySet()
        val plan = kafkaAdminClient.currentAssignment(limitToTopics)
        val weightFn = ProfileBasedWeightFn() // cli.get(w) { w -> WeightFns.values().filter { it.id == w }.map { TODO() } }?: ProfileBasedWeightFn()

        logger.debug { "currentAssignment=$plan" }

        val strategy = getStrategy(cli, kafkaAdminClient, plan, weightFn)
        val newPlan = strategy.newPlan()

        logger.debug { "newPlan=$newPlan" }
        println("New assigment plan:")
        val replicasByPartitionAndTopic = newPlan.partitions
                .map { p -> p.partition to p.topic to p.replicas }
                .toMap()

        plan.partitions
                .groupBy { it.topic }
                .forEach { (topic, partitions) ->
                    println("Topic: $topic")
                    partitions.forEach { p ->
                        val newReplicas = replicasByPartitionAndTopic[Pair(p.partition, topic)]
                        println(String.format(
                                "%1s\t%3s [%s]->[%s] -- %s",
                                if (newReplicas == null) "X" else " ",
                                p.partition,
                                p.replicas.joinToString(),
                                newReplicas?.joinToString() ?: "<untouched>",
                                "ISR: ${p.inSyncReplicas.joinToString()} Leader: ${p.leader ?: "none"}"
                        ))
                    }
                }

        val currentReassignment = kafkaAdminClient.currentReassignment()

        if (currentReassignment != null)
            throw IllegalStateException("Can't start another reassignment. There is in progress one.")

        if (!dryRun) {
            if (!kafkaAdminClient.reassignPartitions(newPlan)) {
                println("ERROR: Can't reassign partitions")
                return
            }
        }
    }

    protected abstract fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment, weightFn: WeightFn): PartitionAssignmentStrategy

    protected abstract fun getOptionList(): List<Option>
}