__author__ = 'rmarkowsky'

# stdlib
from collections import defaultdict

# 3p
from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


# There are 2 goals of replica assignment:
# 1. Spread the replicas evenly among brokers.
# 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
#
# To achieve this goal, we:
# 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
# 2. Assign the remaining replicas of each partition with an increasing shift.
#
# Here is an example of assigning
# broker-0  broker-1  broker-2  broker-3  broker-4
# p0        p1        p2        p3        p4       (1st replica)
# p5        p6        p7        p8        p9       (1st replica)
# p4        p0        p1        p2        p3       (2nd replica)
# p3        p4        p0        p1        p2       (3nd replica)
# p7        p8        p9        p5        p6       (3nd replica)

def assignReplicasToBrokers(broker_list,
                            num_partitions,
                            replication_factor,
                            fixed_start_index=-1,
                            start_partition_id=-1):
    if num_partitions <= 0:
        raise ValueError("number of partitions must be larger than 0")
    if replication_factor <= 0:
        raise ValueError("replication factor must be larger than 0")
    if replication_factor > broker_list.size():
        raise ValueError("replication factor: " + replication_factor +
                                      " larger than available brokers: " + broker_list.size)
    ret = {}
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    for (i <- 0 until nPartitions) {
    if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
    nextReplicaShift += 1
    val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
    var replicaList = List(brokerList(firstReplicaIndex))
    for (j <- 0 until replicationFactor - 1)
    replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
    ret.put(currentPartitionId, replicaList.reverse)
    currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
    }"

