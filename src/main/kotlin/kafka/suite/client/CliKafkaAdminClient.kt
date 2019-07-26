package kafka.suite.client

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import java.io.BufferedInputStream
import java.io.InputStreamReader
import java.lang.IllegalStateException

/**
 * Some old clusters, i.e. 0.10.2.1, can't work with the Scala client,
 * replacing with such ugly workaround, which runs some commands via calling CLI tools
 */
class CliKafkaAdminClient(
        private val bootstrapServer: String,
        private val zkConnectionString: String,
        private val kafkaWorkDir: String
) : KafkaAdminClient {

    private val scalaClient = ScalaKafkaAdminClient(bootstrapServer, zkConnectionString)

    override fun topics(limitToTopics: Set<String>): Map<String, List<Partition>> {
        return topicNames().asSequence()
                .filter { limitToTopics.isEmpty() || it in limitToTopics }
                .mapIndexed { i, topicName ->
                    println("Reading info about `$topicName` topic (${i + 1}/${topicNames().filter { limitToTopics.isEmpty() || it in limitToTopics }.size})")
                    val o = run("$kafkaWorkDir/kafka-topics.sh --zookeeper $zkConnectionString --describe --topic $topicName")
                    val regex = Regex("\\s+Topic:\\s+([-_.\\w]+)\\s+Partition:\\s+([\\d]+).*Replicas:\\s+([,\\d]+).*Isr:\\s+([,\\d]+)")
                    topicName to o
                            .filter { regex.matches(it) }
                            .map { e ->
                                regex.findAll(e)
                                        .map { m ->
                                            val topic = m.groupValues[1]
                                            val partition = m.groupValues[2].toInt()
                                            val replicas = m.groupValues[3].split(",").map { it.trim().toInt() }
                                            val isr = m.groupValues[4].split(",").map { it.trim().toInt() }
                                            Partition(topic, partition, replicas, isr)
                                        }
                                        .toList()
                            }
                            .flatten()
                }
                .toMap()
    }

    override fun brokers(): Map<Int, KafkaBroker> = scalaClient.brokers()

    override fun currentAssignment(limitToTopics: Set<String>, version: Int): KafkaPartitionAssignment {
        return KafkaPartitionAssignment(version, topics(limitToTopics).values.flatten())
    }

    override fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean = scalaClient.reassignPartitions(plan)

    override fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean = scalaClient.isReassignmentFinished(plan)

    private fun topicNames(): List<String> {
        return run("$kafkaWorkDir/kafka-topics.sh --zookeeper $zkConnectionString --list")
    }

    private fun run(command: String): List<String> {
        val exec = Runtime.getRuntime().exec(command)
        val istream = exec.inputStream
        val estream = exec.errorStream
        exec.waitFor()

        val error = InputStreamReader(BufferedInputStream(estream)).readLines()
        if (!error.isEmpty()) throw IllegalStateException("Error during running command `$command`: " + error.joinToString("\n"))

        val o = InputStreamReader(BufferedInputStream(istream)).readLines()
        return o
    }
}