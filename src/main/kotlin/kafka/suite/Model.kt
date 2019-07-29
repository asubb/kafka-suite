package kafka.suite

data class TopicDesc(
        val replicationFactor: Int,
        val appxPartitionSize: Long?
)

data class Partition(
        val topic: String,
        val partition: Int,
        val replicas: List<Int>,
        val inSyncReplicas: List<Int> = replicas,
        val leader: Int? = null
)

data class KafkaPartitionAssignment(
        val version: Int,
        val partitions: List<Partition>
)

data class KafkaBroker(
        val id: Int,
        val address: String,
        val rack: String
)