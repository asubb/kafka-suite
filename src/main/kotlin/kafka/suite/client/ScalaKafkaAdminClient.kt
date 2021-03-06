package kafka.suite.client

import com.fasterxml.jackson.databind.node.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kafka.admin.ReassignPartitionsCommand
import kafka.admin.ReassignmentCompleted
import kafka.admin.ReassignmentFailed
import kafka.admin.`ReassignPartitionsCommand$`
import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import kafka.suite.util.*
import kafka.zk.AdminZkClient
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.data.Stat
import scala.collection.Seq

class ScalaKafkaAdminClient(
        bootstrapServer: String,
        zkConnectionString: String
) : KafkaAdminClient {

    private val mapper = jacksonObjectMapper().registerKotlinModule()

    private val adminClient = AdminClient.create(mapOf("bootstrap.servers" to bootstrapServer))

    private val kafkaZkClient = KafkaZkClient(ZooKeeperClient(
            zkConnectionString,
            30000,
            30000,
            10,
            Time.SYSTEM,
            "group",
            "type"

    ), false, Time.SYSTEM)

    private val zkClient = AdminZkClient(kafkaZkClient)

    override fun topics(limitToTopics: Set<String>): Map<String, List<Partition>> {
        val names = adminClient.listTopics().names().get()
        return adminClient
                .describeTopics(names)
                .all()
                .get()
                .entries.asSequence()
                .filter { limitToTopics.isEmpty() || it.key in limitToTopics }
                .map { e ->
                    e.key!! to e.value!!.partitions().map {
                        Partition(
                                e.key,
                                it.partition(),
                                it.replicas().map { n -> n.id() },
                                it.isr().map { n -> n.id() },
                                it.leader()?.id()
                        )
                    }
                }
                .toMap()
    }

    override fun brokers(): Map<Int, KafkaBroker> {
        return fromScala(kafkaZkClient.allBrokersInCluster)
                .map {
                    it.id() to KafkaBroker(
                            it.id(),
                            fromScala(it.endPoints()).first().host(),
                            it.rack().value() ?: "none"
                    )
                }
                .toMap()
    }

    override fun currentAssignment(limitToTopics: Set<String>, version: Int): KafkaPartitionAssignment {
        val names = adminClient.listTopics().names().get()
        val partitions = adminClient
                .describeTopics(names)
                .all()
                .get()
                .entries.asSequence()
                .filter { limitToTopics.isEmpty() || it.key in limitToTopics }
                .flatMap { e -> e.value.partitions().asSequence().map { e.key to it } }
                .map { e ->
                    Partition(
                            e.first,
                            e.second.partition(),
                            e.second.replicas().map { it.id() },
                            e.second.isr().map { it.id() },
                            e.second.leader()?.id()
                    )
                }
                .toList()
        return KafkaPartitionAssignment(version, partitions)
    }

    override fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean {
        val assignmentPlan = createAssignmentPlan(plan)
        val cmd = ReassignPartitionsCommand(
                kafkaZkClient,
                adminClient.toScalaOption(),
                assignmentPlan,
                emptyMap<TopicPartitionReplica, String>().asScala(),
                zkClient
        )
        return cmd.reassignPartitions(
                `ReassignPartitionsCommand$`.`MODULE$`.NoThrottle(), // TODO provide as a parameter
                30000 // TODO provide as a parameter
        )
    }

    override fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean {
        val assignmentPlan = createAssignmentPlan(plan)
        return fromScala(`ReassignPartitionsCommand$`.`MODULE$`.checkIfPartitionReassignmentSucceeded(
                kafkaZkClient,
                assignmentPlan
        ))
                .entries
                .all { it.value.status() in setOf(ReassignmentCompleted.status(), ReassignmentFailed.status()) }

    }

    @Suppress("UNCHECKED_CAST")
    override fun currentReassignment(): KafkaPartitionAssignment? {
        val partitions = fromScala(kafkaZkClient.partitionReassignment)
                .map { (topicPartition, replicasSeq) ->
                    Partition(topicPartition.topic(), topicPartition.partition(), fromScala(replicasSeq) as List<Int>)
                    // TODO probably it's worth to enrich it with current ISR and Leader info
                }
        return if (partitions.isEmpty())
            null
        else
            KafkaPartitionAssignment(
                    1,
                    partitions
        )
    }

    override fun updatePartitionAssignment(topic: String, partition: Int, replicas: List<Int>, leader: Int) {
        val path = "/brokers/topics/$topic/partitions/$partition/state"

        val stat = Stat()
        val data = kafkaZkClient
                .currentZooKeeper()
                .getData(path, null, stat)

        val j = mapper.readTree(data) as ObjectNode
        j["leader"] = IntNode(leader)
        j["isr"] = ArrayNode(JsonNodeFactory.instance, replicas.map { IntNode(it) })

        val newData = mapper.writeValueAsBytes(j)
        kafkaZkClient
                .currentZooKeeper()
                .setData(path, newData, stat.version)
    }

    private fun createAssignmentPlan(plan: KafkaPartitionAssignment): scala.collection.Map<TopicPartition, Seq<Any>> {
        return plan.partitions.map { partition ->
            TopicPartition(partition.topic, partition.partition) to partition.replicas.map { it as Any }.toSet().asScalaSeq()
        }.toMap().asScala()
    }

}