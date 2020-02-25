package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ReassignmentModule : RunnableModule {

    private val f = Option("f", "force", true, "Forces assignment of partition -- basically tells cluster that reassigning is finished. " +
            "Fixes cases when kafka reassignment stuck, i.e. for 0.10.2 when there is no leader on partition -- it never finishes. \n" +
            "USE WITH CAUTION! If you need to use it the data on partition is completely lost. Also keep an eye on kafka controller logs, " +
            "some brokers may file because of such assignments, you would need to restart or even clean some partitions on broker manually. Follow the logs. \n" +
            "Syntax: topic1:2:3:4,topic2:1 -- force reassignment for topic `topic` partitions 2,3,4 and `topic2` partition 1.")

    private val w = Option("w", "wait", true, "Waits current reassignment to finish. " +
            "If -q flag specified doesn't output anything. As a parameter specify period of checking in seconds")
    private val q = Option("q", "quiet", false, "Do not output anything while waiting.")

    override fun getDescription(): String = "Describes current in progress reassignments. Can apply some fixes."

    override fun module(): Module = Module.REASSIGNMENT

    override fun getOptions(): Options = Options().of(f, w, q)

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        val reassignment = kafkaAdminClient.currentReassignment()

        if (reassignment != null) {
            when {
                cli.has(f) -> forceReassignments(cli, reassignment, kafkaAdminClient)
                cli.has(w) -> {
                    val delayInMs = cli.getRequired(w) { it.toLong() * 1000 }
                    val quiet = cli.has(q)

                    val topics = reassignment.partitions.map { it.topic }.toSet()
                    val assignment = kafkaAdminClient.currentAssignment(topics)

                    val start = System.currentTimeMillis()
                    var c = 0
                    var r = reassignment
                    while (r != null) {
                        if (!quiet) println("Left ${r.partitions.size} partitions. Elapsed ${(System.currentTimeMillis() - start) / 1000.0f}s")
                        if (c++ % 5 == 0) {
                            if (!quiet) printReassignment(assignment, r)
                        }
                        Thread.sleep(delayInMs)
                        r = kafkaAdminClient.currentReassignment()
                    }

                }
                else -> printReassignmentInProgress(reassignment, kafkaAdminClient)
            }
        } else {
            println("No active reassignments")
        }
    }

    private fun forceReassignments(cli: CommandLine, reassignment: KafkaPartitionAssignment, kafkaAdminClient: KafkaAdminClient) {
        val partitions = cli.getRequired(f) {
            it.split(",")
                    .map { tp ->
                        val v = tp.split(":")
                        val topic = v.first()
                        v.drop(1).map { p -> Pair(topic, p.toInt()) }
                    }
                    .flatten()
        }
        val c = reassignment.partitions
                .filter { it.topic to it.partition in partitions }
                .map { p ->
                    println("Forcing reassignment of partition ${p.topic}#${p.partition}")
                    kafkaAdminClient.updatePartitionAssignment(p.topic, p.partition, p.replicas, p.replicas.first())
                    1
                }
                .count()
        println("Forced to reassign $c partitions")
    }

    private fun printReassignmentInProgress(reassignment: KafkaPartitionAssignment, kafkaAdminClient: KafkaAdminClient) {
        val topics = reassignment.partitions.map { it.topic }.toSet()
        val assignment = kafkaAdminClient.currentAssignment(topics)

        printReassignment(assignment, reassignment)
        println("Total: ${reassignment.partitions.size}")
    }

    private fun printReassignment(assignment: KafkaPartitionAssignment, reassignment: KafkaPartitionAssignment) {
        val currentReplicasStateByPartitionAndTopic = assignment.partitions
                .map { p -> p.partition to p.topic to p.replicas }
                .toMap()

        reassignment.partitions
                .sortedBy { it.partition }
                .groupBy { it.topic }
                .forEach { (topic, partitions) ->
                    println("Topic: $topic")
                    partitions.forEach { p ->
                        val current = currentReplicasStateByPartitionAndTopic.getValue(Pair(p.partition, topic))
                        println(String.format(
                                "\t%3s [%s]->[%s]",
                                p.partition,
                                current.joinToString(),
                                p.replicas.joinToString()
                        ))
                    }
                }
    }
}