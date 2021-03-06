package kafka.suite.reassign

import assertk.assertThat
import assertk.assertions.*
import assertk.catch
import kafka.suite.*
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object ChangeReplicationFactorPartitionAssignmentStrategySpec : Spek({

    describe("4-node kafka cluster with one test topic, replication factor=3") {
        val healthy = KafkaPartitionAssignment(1, listOf(
                Partition("test", 1, listOf(1, 2, 3), listOf(1, 2, 3), 1),
                Partition("test", 2, listOf(1, 3, 2), listOf(1, 2, 3), 1),
                Partition("test", 3, listOf(2, 3, 4), listOf(1, 2, 3), 2),
                Partition("test", 4, listOf(3, 4, 2), listOf(1, 2, 3), 3)
        ))

        describe("Decreasing replication factor to 1") {

            val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ -> healthy })
            val plan = client.currentAssignment()

            val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                    plan,
                    brokers(4, 1),
                    MonoWeightFn(),
                    emptySet(),
                    false,
                    1
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()

                it("should contain all partition spread across nodes 1,2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
                }

                it("should have replication factor 1") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(1))
                }
            }
        }

        describe("Decreasing replication factor to 2, rack unaware") {

            val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ -> healthy })
            val plan = client.currentAssignment()

            val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                    plan,
                    brokers(4, 1),
                    MonoWeightFn(),
                    emptySet(),
                    false,
                    2
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()

                it("should contain all partition spread across nodes 1,2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
                }

                it("should have replication factor 2") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(2))
                }
            }
        }

        describe("Decreasing replication factor to 2, 2 racks") {

            val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ -> healthy })
            val plan = client.currentAssignment()

            val brokers = brokers(4, 2)

            val brokerById = brokers.map { it.id to it }.toMap()

            val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                    plan,
                    brokers,
                    MonoWeightFn(),
                    emptySet(),
                    false,
                    2
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()

                it("should contain all partition spread across nodes 1,2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
                }

                it("should have replication factor 2") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(2))
                }

                it("should have each partition on each rack") {
                    assertThat(newPlan.partitions, "Plan: $newPlan").each {
                        it.matchesPredicate { p ->
                            p.replicas
                                    .map { b -> brokerById.getValue(b).rack }
                                    .distinct()
                                    .count() == 2
                        }
                    }
                }
            }
        }

        describe("Increase replication factor to 4, rack unaware") {

            val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ -> healthy })
            val plan = client.currentAssignment()

            val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                    plan,
                    brokers(4, 1),
                    MonoWeightFn(),
                    emptySet(),
                    false,
                    2
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()

                it("should contain all partition spread across nodes 1,2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
                }

                it("should have replication factor 4") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(2))
                }
            }
        }

        describe("Increase replication factor to 4, 4 racks") {

            val brokers = brokers(4, 4)

            val brokerById = brokers.map { it.id to it }.toMap()

            val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ -> healthy })
            val plan = client.currentAssignment()

            val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                    plan,
                    brokers,
                    MonoWeightFn(),
                    emptySet(),
                    false,
                    4
            )

            describe("new plan generated by strategy") {
                val newPlan = strategy.newPlan()

                it("should contain all partition spread across nodes 1,2,3,4") {
                    assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
                }

                it("should have replication factor 4") {
                    assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(4))
                }

                it("should have each partition on each rack") {
                    assertThat(newPlan.partitions, "Plan: $newPlan").each {
                        it.matchesPredicate { p ->
                            p.replicas
                                    .map { b -> brokerById.getValue(b).rack }
                                    .distinct()
                                    .count() == 4
                        }
                    }
                }
            }
        }

    }

    describe("Increase replication factor from 1 rack unaware to 4 on 4 racks") {

        val brokers = brokers(4, 4)

        val brokerById = brokers.map { it.id to it }.toMap()

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 1, listOf(1), listOf(1), 1),
                    Partition("test", 2, listOf(1), listOf(1), 1),
                    Partition("test", 3, listOf(1), listOf(1), 1),
                    Partition("test", 4, listOf(1), listOf(1), 1)
            ))
        })
        val plan = client.currentAssignment()

        val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                brokers,
                MonoWeightFn(),
                emptySet(),
                false,
                4
        )

        describe("new plan generated by strategy") {
            val newPlan = strategy.newPlan()

            it("should contain all partition spread across nodes 1,2,3,4") {
                assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
            }

            it("should have replication factor 4") {
                assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(4))
            }

            it("should have each partition on each rack") {
                assertThat(newPlan.partitions, "Plan: $newPlan").each {
                    it.matchesPredicate { p ->
                        p.replicas
                                .map { b -> brokerById.getValue(b).rack }
                                .distinct()
                                .count() == 4
                    }
                }
            }
        }
    }

    describe("Decrease replication factor from 3 to 2 on 2 racks") {

        val brokers = brokers(4, 2)

        val brokerById = brokers.map { it.id to it }.toMap()

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 1, listOf(1, 3, 2), leader = 1),
                    Partition("test", 2, listOf(2, 4, 1), leader = 2),
                    Partition("test", 3, listOf(3, 1, 4), leader = 3),
                    Partition("test", 4, listOf(4, 2, 3), leader = 4),
                    Partition("test", 5, listOf(3, 2, 4), leader = 3),
                    Partition("test", 6, listOf(3, 1, 2), leader = 3)
            ))
        })
        val plan = client.currentAssignment()

        val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                brokers,
                MonoWeightFn(),
                emptySet(),
                false,
                2
        )

        describe("new plan generated by strategy") {
            val newPlan = strategy.newPlan()

            it("should contain all partition spread across nodes 1,2,3,4") {
                assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").intersects(setOf(1, 2, 3, 4))
            }

            it("should have replication factor 2") {
                assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(2))
            }

            it("should have each partition on each rack") {
                assertThat(newPlan.partitions, "Plan: $newPlan").each {
                    it.matchesPredicate { p ->
                        p.replicas
                                .map { b -> brokerById.getValue(b).rack }
                                .distinct()
                                .count() == 2
                    }
                }
            }
        }
    }

    describe("Decrease replication factor from 3 to 1 on 3 racks if partitions are under replicated") {

        val brokers = brokers(4, 3)
                .filter { it.id == 1 || it.id == 3 } // only 2 brokers are up

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 1, listOf(1, 3, 2), listOf(1, 3), leader = 1),
                    Partition("test", 2, listOf(2, 4, 1), listOf(1), leader = 1),
                    Partition("test", 3, listOf(3, 1, 4), listOf(1, 3), leader = 1),
                    Partition("test", 4, listOf(4, 2, 3), listOf(3), leader = 3),
                    Partition("test", 5, listOf(3, 2, 4), listOf(3), leader = 3),
                    Partition("test", 6, listOf(3, 1, 2), listOf(1, 3), leader = 1)
            ))
        })
        val plan = client.currentAssignment()

        val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                brokers,
                MonoWeightFn(),
                emptySet(),
                true,
                1
        )

        describe("new plan generated by strategy") {
            val newPlan = strategy.newPlan()

            it("should contain all partition spread across nodes 1,3") {
                assertThat(newPlan.partitions.map { it.replicas }.flatten().toSet(), "Plan: $newPlan").isEqualTo(setOf(1, 3))
            }

            it("should have replication factor 1") {
                assertThat(newPlan.partitions.map { it.replicas.distinct().count() }.toSet(), "Plan: $newPlan").isEqualTo(setOf(1))
            }
        }
    }

    describe("Decrease replication factor from 3 to 1 on 3 racks if partitions are under replicated no leader") {

        val brokers = brokers(4, 3)
                .filter { it.id == 1 } // only 1 broker is up

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 4, listOf(4, 2, 3), listOf(2), null),
                    Partition("test", 5, listOf(3, 2, 4), listOf(3), null),
                    Partition("test", 6, listOf(3, 1, 2), listOf(1), 1)
            ))
        })
        val plan = client.currentAssignment()


        val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                brokers,
                MonoWeightFn(),
                emptySet(),
                true,
                1
        )

        describe("new plan generated by strategy") {
            val e = catch { strategy.newPlan() }

            it("should throw an exception") {
                assertThat(e)
                        .isNotNull()
                        .message().matchesPredicate { it != null && it.contains("Can't decrease replication factor") }
            }
        }
    }

    describe("Decrease replication factor from 3 to 1 on 3 racks if partitions are under replicated no leader. Skipping") {

        val brokers = brokers(4, 3)
                .filter { it.id == 1 } // only 1 broker is up

        val client = TestKafkaAdminClient(currentAssignmentFn = { _, _ ->
            KafkaPartitionAssignment(1, listOf(
                    Partition("test", 4, listOf(4, 2, 3), listOf(2), null),
                    Partition("test", 5, listOf(3, 2, 4), listOf(3), null),
                    Partition("test", 6, listOf(3, 1, 2), listOf(1), 1)
            ))
        })
        val plan = client.currentAssignment()

        val strategy = ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                brokers,
                MonoWeightFn(),
                emptySet(),
                true,
                1,
                skipNoLeader = true
        )

        describe("new plan generated by strategy") {
            val newPlan = strategy.newPlan()

            it("should touch only one partition which has leader") {
                assertThat(newPlan.partitions).hasSize(1)
                assertThat(newPlan.partitions[0]).matchesPredicate {
                    it.partition == 6
                            && it.replicas == listOf(1)
                }
            }
        }
    }
})