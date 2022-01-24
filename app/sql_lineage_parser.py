"""
insert into dbms.schema1.table1 (col1,col2,col3) select t0.c1,t1.c2,t3.c3 from table0 t0 inner join table t1 on t1.id=t0.id inner join
table33 t3 on t3.id=t1.id where t0.date>current_date;
------
dbms.schema1.table1.col1 <-- table0.col1
dbms.schema1.table1.col2 <-- table.col2
dbms.schema1.table1.col3 <-- table33.col3
------



"""
from turtle import update
from pglast.visitors import Visitor
from pglast import parser
import logging
import os
import pglast
from pprint import pprint as pp
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)
logger=logging.getLogger("sql_lineage_logger")
              

class Dummy:
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)


class Node:
    def __init__(self,**data):
        self.__dict__.update(data)

class SqlLineageParser:

    def get_lineage(self,sql):
        sql=parser.parse_sql(sql)
        edges=[]
        for root in sql:
            if isinstance(root.stmt,pglast.ast.SelectStmt):
                logger.debug("DETECTED SELECT STMT")
                into_clause=getattr(root.stmt,"intoClause",None) or "Select"
                if into_clause!="Select":
                    into_clause=self.create_table_node_from_range_var(into_clause.rel)
                edges+= self.get_graph_from_select_stmt(root.stmt,into_clause)
            elif isinstance(root.stmt,pglast.ast.InsertStmt):
                logger.debug("DETECTED INSERT STMT")
                edges+= self.get_graph_from_insert_stmt(root.stmt)
            elif isinstance(root.stmt,pglast.ast.UpdateStmt):
                logger.debug("DETECTED UPDATE STMT")
                edges+= self.get_graph_from_update_stmt(root.stmt)
            elif isinstance(root.stmt,pglast.ast.CreateTableAsStmt):
                logger.debug("DETECTED CREATE STMT AS")
                edges+= self.get_graph_from_create_stmt(root.stmt)
            else:
                raise NotImplementedError(f"{str(type(root.stmt))} is yet to be handled")
        return edges
    
    def get_graph_from_select_stmt(self,root,parent='Select'):
        relations={}
        for target in root.targetList: 
            logger.debug(f"WORKING ON COLUMN REFFERENCE {target.val}")
            node=self.create_column_node(target,parent=parent)
            if node is not None:
                if node.relation_ref not in relations.keys():
                    relations[node.relation_ref]=[node]
                else:
                    relations[node.relation_ref].append(node)
        edges=[]
        edges+=self.define_and_link_upstreams(root.fromClause,relations)
        return edges
    
    def get_graph_from_insert_stmt(self,root,parent=None):
        insert_nodes={}
        insert_select={}
        insert_table=Node(type='table',name=root.relation.relname,schema=root.relation.schemaname,insert_table=True)
        
        for range_var in root.cols:
            node=self.create_column_node(range_var,parent=insert_table,insert_stmt_cols=True)
            insert_nodes[node.location]=node
        
        if isinstance(root.selectStmt,pglast.ast.SelectStmt):
            if getattr(root.selectStmt,'valuesLists',None) is not None:
                constants=self.create_nodes_from_values_list(root.selectStmt.valuesLists)
                for node in constants:
                    insert_select[node.location]=node
                return self._link_nodes_by_location(insert_select,insert_nodes)
            else:
                ss_edges=self.get_graph_from_select_stmt(root.selectStmt,parent=insert_table)
            
                for up,down in ss_edges:
                    if getattr(up.parent,'insert_table',None) is not None:
                        insert_select[up.location]=up
                    if getattr(down.parent,'insert_table',None) is not None:
                        insert_select[down.location]=down
        
                ss_edges+=self._link_nodes_by_location(insert_select,insert_nodes)
                return ss_edges

    def get_graph_from_update_stmt(self,update_stmt):
        edges=[]
        insert_table=Node(type="table",name=getattr(update_stmt.relation,"relname",None),schema=getattr(update_stmt.relation,"schemaname",None))
        if getattr(update_stmt,'fromClause',None) is None:
            for const_value in update_stmt.targetList:
                downstream=self.create_column_node(const_value,parent=insert_table,insert_stmt_cols=True)
                upstream=self.create_constant_node(const_value.val)
                edges.append((upstream,downstream))
            return edges
        edges+=self.get_graph_from_select_stmt(update_stmt,parent=insert_table)
        return edges

    def get_graph_from_create_stmt(self,create_stmt):
        query=getattr(create_stmt,'query',None)
        if query is not None:
            target_rel=Node(type="table",name=getattr(create_stmt.into.rel,"relname",None),schema=getattr(create_stmt.into.rel,"schemaname",None))
            return self.get_graph_from_select_stmt(create_stmt.query,parent=target_rel)
        

    def _link_nodes_by_location(self,left_nodes_dict,right_nodes_dict):
        assert len(left_nodes_dict.keys())==len(right_nodes_dict.keys())

        insert_cols=[right_nodes_dict[n] for n in sorted(list(right_nodes_dict.keys()))]
        select_cols=[left_nodes_dict[n] for n in sorted(list(left_nodes_dict.keys()))]
        
        return [edge for edge in zip(select_cols,insert_cols)]

    def create_nodes_from_values_list(self,values_lists):
        constant_nodes=[]
        for value_list in values_lists:
            for value in value_list:
                constant_nodes.append(Node(type='Constant',name=value.val.val,parent='Constant',location=value.location))
        return constant_nodes

    def define_and_link_upstreams(self,rels,nodes):
        edges=[]
        for rel in rels:
            logger.debug(f"WORKING ON RELATION : {rel}")
            if isinstance(rel,pglast.ast.RangeVar):
                if rel.alias is not None:
                    logger.debug(f"FOUND ALIAS WITH NAME {rel.alias.aliasname} ON RELATION ")
                    if rel.alias.aliasname in nodes.keys():
                        logger.debug(f"FOUND MATCH IN NODE LIST USING ALIAS")
                        edges+=self.link_nodes_using_rel_ref(nodes[rel.alias.aliasname],rel)
                if rel.relname in nodes.keys():
                    edges+=self.link_nodes_using_rel_ref(nodes[rel.relation_ref],rel)
                if "*" in nodes.keys():
                    logger.debug(f"FOUND MATCH USING *")
                    edges+=self.link_nodes_using_rel_ref(nodes["*"],rel)

            elif isinstance(rel,pglast.ast.RangeSubselect):
                subq_relation=Dummy(schemaname='SUBQUERY',relname=rel.alias.aliasname)
                edges += self.link_nodes_using_rel_ref(nodes[rel.alias.aliasname],subq_relation)
                edges += self.get_graph_from_select_stmt(rel.subquery,parent=Node(type='table',name=subq_relation.relname,schema=subq_relation.schemaname))

            elif isinstance(rel,pglast.ast.JoinExpr):
                logger.debug("REL IS Join expression")
                logger.debug(f"CONTINUE RECURSIVE LEFT WITH {rel.larg}")
                edges+=self.define_and_link_upstreams([rel.larg],nodes)
                logger.debug(f"CONTINUE RECURSIVE RIGHT WITH {rel.rarg}")
                edges+=self.define_and_link_upstreams([rel.rarg],nodes)

            else:
                raise ValueError(f"{type(rel)} IS YET TO BE HANDLED")
        return edges
    
    def link_nodes_using_rel_ref(self,nodes,relation):
        edges=[]
        for node in nodes:
            logger.debug(f"WORKING ON NODE {node.__dict__}")
            up_node=Node(**{'type':'column',
                            'name':getattr(node,'column_ref',None),
                            'parent':Node(type='table',
                                          name=getattr(relation,'relname',None),
                                          schema=getattr(relation,'schemaname',None))})
            logger.debug(f"LINKING TO UPSTREAM NODE {up_node.__dict__} WITH PARENT NODE {up_node.parent.__dict__}")
            edges.append((up_node,node))
        return edges

    def create_column_node(self,target,parent,insert_stmt_cols=False):
        col_ref=target.val
        if isinstance(target.val,pglast.ast.FuncCall):
            if target.val.funcname[0].val=="count":
                col_ref=target.val.args[0]
        node_data={}
        try:
            node_data.update({'name':target.name})
            logger.debug(f"TARGET NAME {target.name}")
        except:
            logger.debug(f"NO TARGET NAME")
            pass
        if not insert_stmt_cols:
            if len(col_ref.fields)==2:
                col_ref_val=col_ref.fields[1]
                if isinstance(col_ref_val,pglast.ast.A_Star):
                    col_ref_val="*"
                else:
                    col_ref_val=col_ref_val.val
                node_data.update({'column_ref':col_ref_val,
                        'relation_ref':col_ref.fields[0].val})
            elif len(col_ref.fields)==1:
                col_ref_val=col_ref.fields[0]
                if isinstance(col_ref_val,pglast.ast.A_Star):
                    col_ref_val="*"
                else:
                    col_ref_val=col_ref_val.val
                node_data.update({'column_ref':col_ref_val,'relation_ref':'*'})
            else:
                raise Exception("Node can not be defined")

        node_data['type']='column'
        node_data['parent']=parent
        node_data['location']=target.location
        if node_data.get('name') is None:
            node_data['name']=node_data['column_ref']
        
        return Node(**node_data)

    def create_table_node_from_range_var(self,range_var):
        return Node(type="table",name=range_var.relname,schema=getattr(range_var,"schemaname",None))

    def create_constant_node(self,const_value):
        return Node(type='Constant',name=const_value.val.val,parent='Constant',location=const_value.location)

    def normalize_for_nx_graph(self,lineage):
        edges=[]
        for edge in lineage:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            edges.append((ups,dos))
        return edges