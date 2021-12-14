from networkx.drawing import nx_pylab
from pglast import parser, Node
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
class SqlLineageVis:

    def get_qtree(self, sql):
        '''get query tree
        :param sql: string: valid sql code with postgres flavor
        '''
        return parser.parse_sql(sql)

    def get_graph_from_qtree(self, qtree_dict,graph=None,level=0):
        if graph is None:
            graph=nx.DiGraph()
        patch_for_graph = []
        for node, val in qtree_dict.items():
            main_node=f"{node}-{level}"
            if isinstance(val, dict):
                for key in val.keys():
                    patch_for_graph.append((main_node,f"{key}-{level+1}"))
                graph = self.get_graph_from_qtree(val,graph,level+1)
            elif isinstance(val,tuple):
                for n_item in range(len(val)):
                    patch_for_graph.append((main_node, f"{main_node}[{n_item}]-{level+1}"))
                    graph = self.get_graph_from_qtree({f"{main_node}[{n_item}]":val[n_item]},graph,level+1)
            else:
                patch_for_graph.append((main_node, f"{val}-{level+1}"))
        graph.add_edges_from(patch_for_graph)
        return graph



    def create_vis_image(self,graph):
        graph=nx.nx_agraph.to_agraph(graph)
        graph.layout(prog="dot",args="-Grankdir=LR -Nshape=box")
        graph.draw("static/graph.png")
        graph.write("static/graph.dot")


    def vis_sql_lineage(self,sql):
        qtree=self.get_qtree(sql)
        #print(qtree[0]())
        graph=self.get_graph_from_qtree(qtree[0]())

        return self.create_vis_image(graph)
