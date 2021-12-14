import unittest
import sys
import os

sys.path.append("/mnt/c/Users/T430/VisualStudioProjects/sql-lineage-vis")
from app.sql_lineage_vis import SqlLineageVis


class TestGetGraphFromTree(unittest.TestCase):
    def setUp(self) -> None:
        self.sql_vis = SqlLineageVis()
        self.test_dict = {
            "root_node": {
                "leaf1": "leaf2",
                "leaf12": "leaf22",
                "leaf13": {"leaf23": "leaf31"},
            }
        }
        self.excpected = [
            ("root_node", "leaf1"),
            ("root_node", "leaf12"),
            ("root_node","leaf13"),
            ("leaf1", "leaf2"),
            ("leaf12", "leaf22"),
            ("leaf13", "leaf23"),
            ("leaf23", "leaf31"),
        ]

    def test_get_graph_from_qtree(self):
        actual=list(self.sql_vis.get_graph_from_qtree(self.test_dict).edges)
        try:
            self.assertListEqual(sorted(actual),sorted(self.excpected))
        except:
            print("actual")
            print(actual)
            print("="*100)
            print("excpected")
            print(self.excpected)
            assert False
    def test_get_graph_from_qtree_1(self):
        self.test_dict["one_more"]={"nested":{"dict":"and_this_is_value","dict_2":"second_value"}}
        actual=list(self.sql_vis.get_graph_from_qtree(self.test_dict).edges)
        self.excpected+=[("one_more","nested"),("nested","dict"),("nested","dict_2"),
        ("dict","and_this_is_value"),("dict_2","second_value")]
        try:
            self.assertListEqual(sorted(actual),sorted(self.excpected))
        except:
            print("actual")
            print(actual)
            print("="*100)
            print("excpected")
            print(self.excpected)
            assert False

    def test_get_graph_from_qtree_2(self):
        test_dict_2 = {"key1": {"key11": {
            "key12": {"key32": {"last_key": "value1", "last_key1": "last_value2"}}}}}
        
        actual_2 = list(self.sql_vis.get_graph_from_qtree(test_dict_2).edges)

        excpected_2 = [("key1", "key11"), ("key11", "key12"), ("key12", "key32"), ("key32", "last_key"),
                      ("key32", "last_key1"), ("last_key", "value1"), ("last_key1", "last_value2")]
        
        try:
            self.assertListEqual(sorted(actual_2),sorted(excpected_2))
        except:
            print("actual2")
            print(actual_2)
            print("="*100)
            print("excpected2")
            print(excpected_2)
            assert False
    def test_get_graph_from_qtree_3(self):
        test_dict = {'targetList': (
            {'@': 'ResTarget', 'name': None, 'indirection': None, 'val': {
                '@': 'ColumnRef', 'fields': ({'@': 'String', 'val': 'col3'},)}
                }
        ,)
        }
        actual = list(self.sql_vis.get_graph_from_qtree(test_dict).edges)
        excpected = [("targetList", "targetList[0]"), ("targetList[0]", "@"),
                     ("targetList[0]", "name"), ("targetList[0]", "indirection"), ("targetList[0]", "val"), ("val", "@"),
                     ("val", "fields"), ("fields", "fields[0]"), ("fields[0]", "@"), ("fields[0]", "val"), ("@", "String"), 
                     ("val", "col3"), ("@", "ResTarget"), ("name", "None"), ("indirection", "None"), ("@", "ColumnRef")
                     ]
        try:
            self.assertListEqual(sorted(actual),sorted(excpected))
        except:
            print("actual")
            print(sorted(actual))
            print("="*100)
            print("excpected")
            print(sorted(excpected))
            assert False


{'@': 'RawStmt', 'stmt': 
{'@': 'SelectStmt', 'distinctClause': None, 'intoClause': None, 
'targetList': ({'@': 'ResTarget', 'name': None, 'indirection': None, 
'val': {'@': 'ColumnRef', 'fields': 
({'@': 'String', 'val': 'col1'},), 
'location': 7}, 'location': 7},), 'fromClause': 
({'@': 'RangeVar', 'catalogname': None, 'schemaname': None, 
'relname': 'table1', 'inh': True, 'relpersistence': 'p', 'alias': None, 'location': 17},),
                          'whereClause': None, 'groupClause': None, 
                          'havingClause': None, 'windowClause': None, 
                          'valuesLists': None, 'sortClause': None, 
                          'limitOffset': None, 'limitCount': None, 
                          'limitOption': 
                          {'#': 'LimitOption', 
                          'name': 'LIMIT_OPTION_DEFAULT', 'value': 0}, 
                          'lockingClause': None, 'withClause': None, 
                          'op': {'#': 'SetOperation', 'name': 'SETOP_NONE', 'value': 0},
                           'all': False, 'larg': None, 'rarg': None}, 'stmt_location': 0, 'stmt_len': 0}
