import unittest
import simplejson as json

from pytopic import assignReplicasToBrokers

class TestPyTopic(unittest.TestCase):

    def encode_complex(obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        raise TypeError(repr(obj) + " is not JSON serializable")

    def test_replica_assignment(self):
        expected_assignment = {
            '0':[0, 1, 3],
            '1':[1, 2, 4],
            '2':[2, 3, 0],
            '3':[3, 4, 1],
            '4':[4, 0, 2],
            '5':[0, 2, 4],
            '6':[1, 3, 0],
            '7':[2, 4, 1],
            '8':[3, 0, 2],
            '9':[4, 1, 3]
        }
        broker_list = [0,1,2,3,4]
        actualAssignment = assignReplicasToBrokers(broker_list, 10, 3, 0)
        for partition, replica_assigned in actualAssignment.iteritems():
            assert( any(map(lambda v: v in expected_assignment[partition], replica_assigned)))
        jsonstuff = json.JSONEncoder(default=self.encode_complex).encode(actualAssignment)
        json.dumps(jsonstuff)
       
