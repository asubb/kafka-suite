package kafka.suite.reassign

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import kafka.suite.TestKafkaAdminClient
import kafka.suite.brokers
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

class FixNoLeaderPartitionAssignmentStrategySpec : Spek({
    describe("Partitions with no leader (No ISR). 2 replicas. Rack unaware") {

        val brokers = brokers(4, 1)
                .filter { it.id in setOf(1, 4) } // 2 brokers are alive

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 1, listOf(3, 2), listOf(2), null),
                    Partition("test", 2, listOf(3, 2), listOf(3), null),
                    Partition("test", 3, listOf(1, 2), listOf(1), 1),
                    Partition("test", 4, listOf(1, 3), listOf(1), 1)
            ))
        })


        val strategy = FixNoLeaderPartitionAssignmentStrategy(
                client,
                emptySet(),
                brokers,
                { 1 }, // all partitions are the same
                Comparator { _, _ -> 0 } // evenly balanced doesn't matter now
        )

        val newPlan = strategy.newPlan()

        it("Partitions with no ISR should be fixed by moving to healthy nodes") {
            assertThat(newPlan.partitions.size).isEqualTo(2)
            assertThat(newPlan.partitions.first { it.partition == 1 }.replicas.toSet()).isEqualTo(setOf(1, 4))
            assertThat(newPlan.partitions.first { it.partition == 2 }.replicas.toSet()).isEqualTo(setOf(1, 4))
        }

        it("Healthy partitions shouldn't be touched") {
            assertThat(newPlan.partitions.size).isEqualTo(2)
            assertThat(newPlan.partitions.filter{ it.partition == 3 }).isEmpty()
            assertThat(newPlan.partitions.filter{ it.partition == 4 }).isEmpty()
        }
    }

    describe("Partitions with no leader (No ISR). 2 replicas. 2 racks") {

        val brokers = brokers(5, 2)
                .filter { it.id in setOf(1, 3, 4) } // 2 brokers from one rack, 1 broker from another rack are available.

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 1, listOf(2, 5), listOf(2), null),
                    Partition("test", 2, listOf(5, 2), listOf(5), null),
                    Partition("test", 3, listOf(1, 4), listOf(1), 1),
                    Partition("test", 4, listOf(1, 4), listOf(1), 1)
            ))
        })


        val strategy = FixNoLeaderPartitionAssignmentStrategy(
                client,
                emptySet(),
                brokers,
                { 1 }, // all partitions are the same
                Comparator { _, _ -> 0 } // evenly balanced doesn't matter now
        )

        val newPlan = strategy.newPlan()

        it("Partitions with no ISR should be fixed by moving to healthy nodes on different racks")   {
            assertThat(newPlan.partitions.size).isEqualTo(2)
            assertThat(newPlan.partitions.first { it.partition == 1 }.replicas.toSet()).isEqualTo(setOf(1, 4))
            assertThat(newPlan.partitions.first { it.partition == 2 }.replicas.toSet()).isEqualTo(setOf(1, 4))
        }

        it("Healthy partitions shouldn't be touched") {
            assertThat(newPlan.partitions.size).isEqualTo(2)
            assertThat(newPlan.partitions.filter{ it.partition == 3 }).isEmpty()
            assertThat(newPlan.partitions.filter{ it.partition == 4 }).isEmpty()
        }
    }
})