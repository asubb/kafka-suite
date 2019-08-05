package kafka.suite.client

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.utils.Time
import java.io.BufferedInputStream
import java.io.InputStreamReader
import java.lang.IllegalStateException

/**
 * Some old clusters, i.e. 0.10.2.1, can't work with the Scala client,
 * can read some info from ZK directly.
 */
class ZkKafkaAdminClient(
        private val bootstrapServer: String,
        private val zkConnectionString: String
) : KafkaAdminClient {

    private val scalaClient = ScalaKafkaAdminClient(bootstrapServer, zkConnectionString)

    private val zkClient = KafkaZkClient(ZooKeeperClient(
            zkConnectionString,
            30000,
            30000,
            10,
            Time.SYSTEM,
            "group",
            "type"

    ), false, Time.SYSTEM)

    private val mapper = jacksonObjectMapper().registerKotlinModule()


    private data class ZkTopicDesc(
            val version: Int,
            val partitions: Map<String, List<Int>> // partition.intAsString: [replicas.int]
    )

    private data class PartitionDesc(
            @JsonProperty("controller_epoch") val controllerEpoch: Int,
            val leader: Int,
            val version: Int,
            @JsonProperty("leader_epoch") val leaderEpoch: Int,
            val isr: List<Int>
    )


    override fun topics(limitToTopics: Set<String>): Map<String, List<Partition>> {
        val zk = zkClient.currentZooKeeper()
        return zk.getChildren("/brokers/topics", null)
                .filter { it in limitToTopics || limitToTopics.isEmpty() }
                .map { topic ->
                    requireNotNull(topic)
                    val replicas = mapper.readValue<ZkTopicDesc>(
                            zk.getData("/brokers/topics/$topic", null, null)
                    )
                            .partitions
                            .map { it.key.toInt() to it.value }
                            .toMap()
                    val partitionDesc = zk.getChildren("/brokers/topics/$topic/partitions", null)
                            .map { p ->
                                val desc = mapper.readValue<PartitionDesc>(
                                        zk.getData("/brokers/topics/$topic/partitions/$p/state", null, null)
                                )
                                p.toInt() to desc
                            }
                            .toMap()
                    topic to replicas.keys.map { p ->
                        val desc = partitionDesc.getValue(p)
                        Partition(topic, p, replicas.getValue(p), desc.isr, if (desc.leader == -1) null else desc.leader)
                    }
                }
                .toMap()
    }

    override fun brokers(): Map<Int, KafkaBroker> = scalaClient.brokers()

    override fun currentAssignment(limitToTopics: Set<String>, version: Int): KafkaPartitionAssignment {
        return KafkaPartitionAssignment(version, topics(limitToTopics).values.flatten())
    }

    override fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean = scalaClient.reassignPartitions(plan)

    override fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean = scalaClient.isReassignmentFinished(plan)

    override fun currentReassignment(): KafkaPartitionAssignment? = scalaClient.currentReassignment()

    override fun updatePartitionAssignment(topic: String, partition: Int, replicas: List<Int>, leader: Int) = scalaClient.updatePartitionAssignment(topic, partition, replicas, leader)
}