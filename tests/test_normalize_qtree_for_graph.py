import unittest
import sys
import os

sys.path.append("/mnt/c/Users/T430/IdeaProjects/sql-lineage-vis")
from app.sql_lineage_vis import SqlLineageVis


class TestNormalizeQtreeForGraph(unittest.TestCase):
    def test_normalize_qtree_for_graph(self):
        sql_vis = SqlLineageVis()
        test_dict = {
            "root_node": {
                "leaf1": "leaf2",
                "leaf12": "leaf22",
                "leaf13": {"leaf23": "leaf31"},
            }
        }
        excpected = [
            ("root_node", "leaf1"),
            ("root_node", "leaf12"),
            ("root_node","leaf13"),
            ("leaf1", "leaf2"),
            ("leaf12", "leaf22"),
            ("leaf13", "leaf23"),
            ("leaf23", "leaf31"),
        ]
        actual=sql_vis.normalize_qtree_for_graph(test_dict)
        try:
            assert actual == excpected
        except:
            print("actual")
            print(actual)
            print("="*100)
            print("excpected")
            print(excpected)
            assert False
        
