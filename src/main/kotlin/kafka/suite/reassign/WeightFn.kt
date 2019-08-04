package kafka.suite.reassign

import kafka.suite.ClusterProfile
import kafka.suite.Partition

enum class WeightFns(val id: String) {
    PROFILE("profile"),
    MONO("mono")
}

interface WeightFn {

    fun weight(partition: Partition): Long
}

class ProfileBasedWeightFn(
        private val sizePriority: Int = 5,
        private val cpuCreditsPriority: Int = 1,
        private val memoryCreditsPriority: Int = 1,
        private val readRatePriority: Int = 3,
        private val writeRatePriority: Int = 5
) : WeightFn {

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

        val t = (sizePriority + cpuCreditsPriority + memoryCreditsPriority + readRatePriority + writeRatePriority).toDouble()

        val sizeFactor = sizePriority / t
        val cpuCreditsFactor = cpuCreditsPriority / t
        val memoryCreditsFactor = memoryCreditsPriority / t
        val readRateFactor = readRatePriority / t
        val writeRateFactor = writeRatePriority / t

        val r = (w?.size ?: 0L) * sizeFactor +
                (w?.cpuCredits?.toLong() ?: 0L) * cpuCreditsFactor +
                (w?.memoryCredits?.toLong() ?: 0L) * memoryCreditsFactor +
                (w?.readRate ?: 0L) * readRateFactor +
                (w?.writeRate ?: 0L) * writeRateFactor

        return if (r <= 0) 1 else r.toLong()
    }
}


class MonoWeightFn : WeightFn {
    override fun weight(partition: Partition): Long = 1 // all partitions are the same
}

