from networkx.drawing import nx_pylab
from pglast import parser, Node
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
import uuid
import pprint as pp
from .utils import Utils

RELATION_STRING="-->"
class SqlLineageVis:

    def get_qtree(self, sql):
        '''get query tree
        :param sql: string: valid sql code with postgres flavor
        '''
        return parser.parse_sql(sql)

    def get_graph_from_flat_dict(flat_dict,sep="."):
        graph=[]
        for k,v in flat_dict:
            if not isinstance(v,(str,int)):
                raise ValueError(f"{v} is not a valid, nested dict are not supported")
            item=k+sep+v

            items=k.split(sep)
            for i in range(len(items)):
                graph.appenditems[i]

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


    def get_graph_from_qtree(self, qtree_dict,graph=None,id=True):
        if graph is None:
            graph=nx.DiGraph()
        patch_for_graph = []
        for node, val in qtree_dict.items():
            if id:
                id=uuid.uuid4()
                main_node=f"{node}-{id}"
            else:
                main_node=f"{node}"
            
            if isinstance(val, dict):
                for key in val.keys():
                    patch_for_graph.append((main_node,f"{key}-{uuid.uuid4()}"))
                graph = self.get_graph_from_qtree(val,graph,id=False)
            elif isinstance(val,tuple):
                for n_item in range(len(val)):
                    tuple_key_id=uuid.uuid4()
                    patch_for_graph.append((main_node, f"{main_node}[{n_item}]-{tuple_key_id}"))
                    graph = self.get_graph_from_qtree({f"{main_node}[{n_item}]-{tuple_key_id}":val[n_item]},graph,id=False)
            else:
                patch_for_graph.append((main_node, f"{val}-{uuid.uuid4()}"))
        graph.add_edges_from(patch_for_graph)
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
        
        qtree=self.get_qtree(sql)[0]()
        pp.pprint(qtree)
        print("*"*59)
        qtree_flatten=Utils.flatten_dict(qtree,sep=RELATION_STRING)
        pp.pprint(qtree_flatten)
        graph=self.get_nxgraph_from_flat_dict(qtree_flatten,separator=RELATION_STRING)

        return self.create_vis_image(graph)
