from networkx.drawing import nx_pylab
from pglast import parser, Node
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
import uuid
import pprint as pp
from .utils import Utils
from app.sql_lineage_parser import SqlLineageParser

RELATION_STRING="-->"
class SqlLineageVis:

    def get_qtree(self, sql):
        '''get query tree
        :param sql: string: valid sql code with postgres flavor
        '''
        return parser.parse_sql(sql)

    def get_nxgraph_from_string_encoded_gpath(self,string_encoded_gpath,sep,graph=None):
        if string_encoded_gpath=="":
            raise ValueError(f"No encoded graph in string {string_encoded_gpath} using separator: {sep}")
        if graph is None:
            graph = nx.DiGraph()
        nodes = string_encoded_gpath.count(sep)
        if nodes==0:
            graph.add_node(string_encoded_gpath)
            return graph
        left=string_encoded_gpath.rsplit(sep,maxsplit=nodes)[0]
        for num in range(1,nodes+1):
            #print("num is ",num)
            right=string_encoded_gpath.rsplit(sep,maxsplit=nodes-num)[0]
            #print("right is ",right)
            graph.add_edge(left,right)
            left=right
        return graph



    def get_nxgraph_from_flat_dict(self, flat_dict,separator,graph=None,id=True):
        if graph is None:
            graph=nx.DiGraph()

        for k,v in flat_dict.items():
            
            string_encoded_gpath=k+separator+str(v)
            graph=self.get_nxgraph_from_string_encoded_gpath(string_encoded_gpath,sep=separator,graph=graph)

        return graph

    def create_vis_image(self,graph):
        graph=nx.nx_agraph.to_agraph(graph)
        graph.layout(prog="dot",args="-Grankdir=LR -Nshape=box")
        graph.draw("static/graph.png")
        #graph.write("static/graph.dot")


    def vis_sql_lineage(self,sql):
        
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage(sql)
        edges_for_graph=lineage_parser.normalize_for_nx_graph(val)
        graph=nx.DiGraph()
        graph.add_edges_from(edges_for_graph)
        return self.create_vis_image(graph)

    def vis_sql_tree(self,sql):
        qtree=self.get_qtree(sql)[0]()
        pp.pprint(qtree)
        print("*"*59)
        qtree_flatten=Utils.flatten_dict(qtree,sep=RELATION_STRING)
        pp.pprint(qtree_flatten)
        graph=self.get_nxgraph_from_flat_dict(qtree_flatten,separator=RELATION_STRING)
        return self.create_vis_image(graph)