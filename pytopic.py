__author__ = 'rmarkowsky'

# stdlib
from collections import defaultdict
from random import randint

# 3p
#from kafka.client import KafkaClient
#from kafka.common import OffsetRequest
#from kazoo.client import KazooClient
#from kazoo.exceptions import NoNodeError

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

    # initialize replica assignment arrays
    for p in range (0, num_partitions):
        ret[str(p)] = [0] * replication_factor

    shift = 0
    num_brokers = len(broker_list)
    for replica in range (0, replication_factor):
        for p in range (0,num_partitions):
            position = (num_partitions * replica + p)
            if position % num_brokers == 0 and replica > 0:
                shift += 1
            ret[str(p)][replica] = (position + shift) % num_brokers

    return ret


