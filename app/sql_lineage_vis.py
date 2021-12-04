from pglast import parser, Node
import networkx as nx
import matplotlib.pyplot as plt

class SqlLineageVis:

    def get_qtree(self, sql):
        '''get query tree
        :param sql: string: valid sql code with postgres flavor
        '''
        return parser.parse_sql(sql)

    def normalize_qtree_for_graph(self, qtree_dict):
        patch_for_graph = []
        for node, val in qtree_dict.items():
            if isinstance(val, dict):
                patch_for_graph += [(node, key) for key in val.keys()]
                patch_for_graph += self.normalize_qtree_for_graph(val)
            else:
                patch_for_graph.append((node, val))
        return patch_for_graph

    def create_vis_image(self,graph):
        g=nx.DiGraph()
        pass


    def vis_sql_lineage(self,sql):
        qtree=self.get_qtree(sql)
        graph_items=self.normalize_qtree_for_graph(qtree[0]())
        return self.create_vis_image(graph_items)


