import unittest
import sys


sys.path.append("/mnt/c/Users/T430/VisualStudioProjects/sql-lineage-vis")
from app.utils import Utils

class TestUtils(unittest.TestCase):

    def test_flatten_dict(self):
        case1={"key1":{"key2":{"key3":{"key1":"value1"}}}}
        case1_e={"key1.key2.key3.key1":"value1"}
        actual=Utils.flatten_dict(case1,sep=".")

        self.assertDictEqual(case1_e,actual)


        case2={"key1":{"key2":{"key3":{"key1":"value1"}}},"key2":{"key2":"value2"},"key3":123}
        case2_e={"key1.key2.key3.key1":"value1",
                "key2.key2":"value2",
                "key3":123}

        actual=Utils.flatten_dict(case2,sep=".")

        self.assertDictEqual(case2_e,actual)

        case3={"key1":{"key2":{"key3":{"key1":"value1","key2":"value2"},"key33":{"key13":"value13"}}},
        "key2":{"key2":"value2"},
        "key3":123,
        "tup1":({"key1":{"key2":"val2"}},
                11,
                ({"tup1":"nested"},33)),
        "last":{"tuple":(1,3)}
        }

        case3_e={"key1.key2.key3.key1":"value1",
                "key1.key2.key3.key2":"value2",
                "key1.key2.key33.key13":"value13",
                "key2.key2":"value2",
                "key3":123,
                "tup1[0].key1.key2":"val2",
                "tup1[1]":11,
                "tup1[2][0].tup1":"nested",
                "tup1[2][1]":33,
                "last.tuple[0]":1,
                "last.tuple[1]":3}

        actual=Utils.flatten_dict(case3,sep=".")
        
        print(case3_e)
        print("*"*100)
        print(actual)
        self.assertDictEqual(case3_e,actual)


        
