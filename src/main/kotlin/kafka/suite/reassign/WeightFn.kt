package kafka.suite.reassign

import kafka.suite.ClusterProfile
import kafka.suite.Partition

enum class WeightFns(val id: String, val creatorFn: () -> WeightFn) {
    PROFILE("profile", { ProfileBasedWeightFn() }),
    MONO("mono", { MonoWeightFn() })
}

interface WeightFn {

    fun weight(partition: Partition): Long
}

class ProfileBasedWeightFn : WeightFn {

    private val weights =
            ClusterProfile.loadActive().weights
                    ?.flatMap { e ->
                        val s = e.key.split(":")
                        val topic = s[0]
                        val weightKeys = if (s.size > 1)
                            s.drop(1)
                                    .map { p -> "$topic:${p.toInt()}" to e.value }
                        else
                            listOf(topic to e.value)
                        weightKeys
                    }
                    ?.toMap()

    override fun weight(partition: Partition): Long {
        val w = weights?.get("${partition.topic}:${partition.partition}")
                ?: weights?.get(partition.topic)

        val r = (w?.size ?: 0L) +
                (w?.cpuCredits?.toLong() ?: 0L) +
                (w?.memoryCredits?.toLong() ?: 0L) +
                (w?.readRate ?: 0L) +
                (w?.writeRate ?: 0L)

        return if (r <= 0) 1 else r
    }
}


class MonoWeightFn : WeightFn {
    override fun weight(partition: Partition): Long = 1 // all partitions are the same
}

