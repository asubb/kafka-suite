package kafka.suite.reassign

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import kafka.suite.*
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object ReplaceAbsentNodesPartitionAssignmentStrategySpec : Spek({

    describe("4-node kafka cluster with one test topic, replication factor=2") {
        val healthy = KafkaPartitionAssignment(1, listOf(
                Partition("test", 1, listOf(1, 2)),
                Partition("test", 2, listOf(1, 3)),
                Partition("test", 3, listOf(2, 3)),
                Partition("test", 4, listOf(3, 4))
        ))


        describe("Replacing 1st node as it's down, 2 racks") {

            val client = TestKafkaAdminClient(
                    currentAssignmentFn = { _, version ->
                        KafkaPartitionAssignment(version, healthy.partitions.map { p ->
                            p.copy(replicas = p.replicas.filter { it != 1 }) // emulate 1 node is down
                        })
                    }
            )

            val strategy = ReplaceAbsentNodesPartitionAssignmentStrategy(
                    client,
                    emptySet(),
                    listOf(
                            KafkaBroker(2, "127.0.0.2", "1"),
                            KafkaBroker(3, "127.0.0.3", "2"),
                            KafkaBroker(4, "127.0.0.4", "2")
                    ),
                    { 1 }, // all partitions are the same
                    Comparator { _, _ -> 0 } // evenly balanced doesn't matter now
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()
                println(newPlan)

                it("shouldn't contain the 1st node") {
                    assertThat(newPlan.partitions.filter { 1 in it.replicas }).isEmpty()
                }

                it("should contain all partition spread across nodes 2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet()).intersects(setOf(2, 3, 4))
                }

                it("should have replication factor 2") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet()).isEqualTo(setOf(2))
                }
            }
        }

        describe("Replacing 1st node as it's down, rack unaware") {

            val client = TestKafkaAdminClient(
                    currentAssignmentFn = { _, version ->
                        KafkaPartitionAssignment(version, healthy.partitions.map { p ->
                            p.copy(replicas = p.replicas.filter { it != 1 }) // emulate 1 node is down
                        })
                    }
            )

            val strategy = ReplaceAbsentNodesPartitionAssignmentStrategy(
                    client,
                    emptySet(),
                    listOf(
                            KafkaBroker(2, "127.0.0.2", "none"),
                            KafkaBroker(3, "127.0.0.3", "none"),
                            KafkaBroker(4, "127.0.0.4", "none")
                    ),
                    { 1 }, // all partitions are the same
                    Comparator { _, _ -> 0 } // evenly balanced doesn't matter now
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()
                println(newPlan)

                it("shouldn't contain the 1st node") {
                    assertThat(newPlan.partitions.filter { 1 in it.replicas }).isEmpty()
                }

                it("should contain all partition spread across nodes 2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet()).intersects(setOf(2, 3, 4))
                }

                it("should have replication factor 2") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet()).isEqualTo(setOf(2))
                }
            }
        }
    }
})