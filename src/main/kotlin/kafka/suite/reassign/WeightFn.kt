package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.Partition

enum class WeightFns(val id: String) {
    PROFILE("profile"),
    MONO("mono")
}

interface WeightFn {

    fun weight(partition: Partition): Int

    fun comparator(): Comparator<Pair<KafkaBroker, Partition>>
}

class ProfileBasedWeightFn : WeightFn {
    override fun weight(partition: Partition): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun comparator(): Comparator<Pair<KafkaBroker, Partition>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}


class MonoWeightFn : WeightFn {

    override fun weight(partition: Partition): Int = 1 // all partitions are the same

    override fun comparator(): Comparator<Pair<KafkaBroker, Partition>> = Comparator { _, _ -> 0 } // all brokers are equivalent

}

