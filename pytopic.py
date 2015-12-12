__author__ = 'rmarkowsky'

# stdlib
from collections import defaultdict
from random import randint

# 3p
#from kafka.client import KafkaClient
#from kafka.common import OffsetRequest
#from kazoo.client import KazooClient
#from kazoo.exceptions import NoNodeError

def replica_index (first_replica_index, second_replica_shift, replica_index, number_brokers):
    shift = 1 + (second_replica_shift + replica_index) % (number_brokers - 1)
    return (first_replica_index + shift) % number_brokers


def assignReplicasToBrokers(broker_list,
                            num_partitions,
                            replication_factor,
                            fixed_start_index=-1,
                            start_partition_id=-1):
    if num_partitions <= 0:
        raise ValueError("number of partitions must be larger than 0")
    if replication_factor <= 0:
        raise ValueError("replication factor must be larger than 0")
    if replication_factor > len(broker_list):
        raise ValueError("replication factor: " + replication_factor +
                                      " larger than available brokers: " + broker_list.size)


    #val ret = new mutable.HashMap[Int, List[Int]]()
    ret = {}
    if fixed_start_index >= 0:
        start_index = fixed_start_index
    else:
        start_index = randint(0,len(broker_list))

    if start_partition_id >= 0:
        current_partition_id = start_partition_id
    else:
        current_partition_id = 0

    if fixed_start_index >= 0:
        next_replica_shift = fixed_start_index
    else:
        next_replica_shift = randint(0,len(broker_list))


    for i in range (0, num_partitions):
        if current_partition_id > 0 and current_partition_id % len(broker_list) == 0:
            next_replica_shift += 1
            first_replica_index = (current_partition_id + start_index) % len(broker_list)
            replica_list = broker_list[first_replica_index:] #List(brokerList(first_replica_index))
            for j in range (0,replication_factor - 1):
                #replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
                replica_list = broker_list[replica_index(first_replica_index,next_replica_shift,j,len(broker_list)):]
            ret[current_partition_id] = replica_list.reverse
            current_partition_id = current_partition_id + 1

    return ret


