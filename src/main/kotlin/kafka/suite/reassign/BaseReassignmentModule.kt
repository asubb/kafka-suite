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
    private val w = Option("w", "weightFn", true, "The name of the weight function: " + WeightFns.values().joinToString { it.id } + ". By default ${WeightFns.MONO.id}")
    private val v = Option("v", "verbose", false, "Print different information regarding reassignment.")

    override fun getOptions(): Options = Options().of(t, w, v, *getOptionList().toTypedArray())

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        val limitToTopics = cli.get(t) { it.split(",").toSet() } ?: emptySet()
        val plan = kafkaAdminClient.currentAssignment()
        val weightFn = cli.get(w) { w -> WeightFns.values().first { it.id == w }.creatorFn() } ?: MonoWeightFn()
        val verbose = cli.has(v)

        logger.debug { "currentAssignment=$plan" }

        val strategy = getStrategy(cli, kafkaAdminClient, plan, weightFn)

        if (verbose) println("Load before: \n${loadToString(strategy)}")

        val newPlan = strategy.newPlan(limitToTopics)

        logger.debug { "newPlan=$newPlan" }
        println("New assigment plan:")
        val replicasByPartitionAndTopic = newPlan.partitions
                .map { p -> p.partition to p.topic to p.replicas }
                .toMap()

        plan.partitions
                .filter { it.topic in limitToTopics || limitToTopics.isEmpty() }
                .sortedWith(compareBy<Partition> { it.topic }.thenBy { it.partition })
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

        if (verbose) println("Load after: \n${loadToString(strategy)}")

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

    private fun loadToString(strategy: PartitionAssignmentStrategy): String =
            strategy.brokerLoadTracker.getLoad().entries
                    .joinToString(separator = "\n") { "\t${it.key.id} (${it.key.address}) -- ${it.value}" }

    protected abstract fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment, weightFn: WeightFn): PartitionAssignmentStrategy

    protected abstract fun getOptionList(): List<Option>
}