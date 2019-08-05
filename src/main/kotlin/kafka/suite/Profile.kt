package kafka.suite

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

data class PartitionWeight(
        val size: Long?,
        val cpuCredits: Int?,
        val memoryCredits: Int?,
        val writeRate: Long?,
        val readRate: Long?
) {
    companion object {
        val empty = PartitionWeight(null, null, null, null, null)
    }
}

data class ClusterProfile(
        val name: String,
        val active: Boolean,
        /** Comma-separated list of ZK servers, i.e. `zoo1:2181,zoo2:2181/root`. */
        val zookeeper: String,
        /** Comma-separated list of brokers, i.e. `kafka1:9092,127.0.0.1:9092`. */
        val brokers: String,
        /** For older version clusters the path to kafka cli binaries is required. */
        val kafkaBin: String,
        /** Weights for topics/partitions, key format is `topic1:1:2` -- defines for specific partitions of the topic, if partitions are absent, defines for all topic partitions. */
        val weights: Map<String, PartitionWeight>? = null,
        /** Use ZK client which performs some actions in ZK directly as Kafka client doesn't provide needed functionality. */
        val zkClient: Boolean? = null
) {

    companion object {

        private val profileDir = File(System.getProperty("user.home"))

        private val mapper: ObjectMapper = jacksonObjectMapper().setDefaultPrettyPrinter(DefaultPrettyPrinter())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

        fun readAll(): List<ClusterProfile> {

            val profilesFile = getProfileFile()

            val content = profilesFile.readText()

            return mapper.readValue(
                    if (content.isEmpty()) "[]" else content
            )
        }

        fun read(name: String): ClusterProfile? {
            return readAll().firstOrNull { it.name.toLowerCase() == name.toLowerCase() }
        }

        fun loadActive(): ClusterProfile {
            return readAll().firstOrNull { it.active }
                    ?: throw IllegalStateException("Can't find active profile, please create one")
        }

        private fun getProfileFile(): File {
            if (!profileDir.exists()) throw IllegalStateException("Can't find home directory: $profileDir")

            val profilesFile = File(profileDir.absolutePath + "/.ksuite.json")

            if (!profilesFile.exists() && !profilesFile.createNewFile())
                throw IllegalStateException("Can't create or read profiles file $profilesFile")
            return profilesFile
        }
    }

    fun save() {
        val profiles = if (this.active) {
            readAll().map { it.copy(active = false) }
        } else {
            readAll()
        }.filter { it.name != this.name } + this

        mapper.writeValue(getProfileFile(), profiles)
    }

}