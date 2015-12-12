__author__ = 'rmarkowsky'

# stdlib
from collections import defaultdict
from random import randint

# 3p
from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


def replicaIndex (first_replica_index, second_replica_shift, replica_index, number_brokers):
    shift = 1 + (second_replica_shift + replica_index) % (number_brokers - 1)
    return (first_replica_index + shift) % number_brokers


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


    #val ret = new mutable.HashMap[Int, List[Int]]()
    ret = {}
    if fixed_start_index >= 0:
        start_index = fixed_start_index
    else:
        start_index = randint(0,broker_list.size)

    if start_partition_id >= 0:
        current_partition_id = start_partition_id
    else:
        current_partition_id = 0

    if fixed_start_index >= 0:
        next_replica_shift = fixed_start_index
    else:
        next_replica_shift = randint(0,broker_list.size)


    for i in range (0, num_partitions):
        if current_partition_id > 0 and current_partition_id % broker_list.size == 0:
            next_replica_shift += 1
            first_replica_index = (current_partition_id + start_index) % broker_list.size
            replica_list = broker_list[first_replica_index:] #List(brokerList(first_replica_index))
            for j in range (0,replication_factor - 1):
                #replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
                replica_list = broker_list[replicaIndex(first_replica_index,next_replica_shift,j,broker_list.size):]
            ret.put(current_partition_id, replica_list.reverse)
            current_partition_id = current_partition_id + 1

    return ret.toMap


