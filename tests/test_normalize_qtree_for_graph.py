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

    def test_get_nxgraph_from_string_encoded_gpath(self):
        case1="root.is.root.of.these[0].nodes"
        case2="root.is.root.of.these[1].and.these.nodes"
        case3="root"
        case4="root.is"
        case5="root==>is"

        exp1=[("root","root.is"),("root.is","root.is.root"),("root.is.root","root.is.root.of"),("root.is.root.of","root.is.root.of.these[0]"),("root.is.root.of.these[0]","root.is.root.of.these[0].nodes")]
        actu1=list(self.sql_vis.get_nxgraph_from_string_encoded_gpath(case1,".").edges)
        self.assertListEqual(exp1,actu1)
        exp2=[("root","root.is"),("root.is","root.is.root"),("root.is.root","root.is.root.of"),("root.is.root.of","root.is.root.of.these[1]"),
        ("root.is.root.of.these[1]","root.is.root.of.these[1].and"),("root.is.root.of.these[1].and","root.is.root.of.these[1].and.these"),("root.is.root.of.these[1].and.these","root.is.root.of.these[1].and.these.nodes")]
        actu2=list(self.sql_vis.get_nxgraph_from_string_encoded_gpath(case2,".").edges)
        self.assertListEqual(exp2,actu2)
        
        exp3=["root"]
        actu3=list(self.sql_vis.get_nxgraph_from_string_encoded_gpath(case3,".").nodes)
        
        self.assertListEqual(exp3,actu3)

        exp4=[("root","root.is")]
        actu4=list(self.sql_vis.get_nxgraph_from_string_encoded_gpath(case4,".").edges)
        
        self.assertListEqual(exp4,actu4)

        try:
            self.sql_vis.get_nxgraph_from_string_encoded_gpath("",".")
        except ValueError as ve:
            self.assertTrue(str(ve).startswith("No encoded graph in string"))
        else:
            assert False
        exp5=[("root","root==>is")]
        actu5=list(self.sql_vis.get_nxgraph_from_string_encoded_gpath(case5,"==>").edges)
        
        self.assertListEqual(exp5,actu5)
        

    

        