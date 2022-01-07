"""
insert into dbms.schema1.table1 (col1,col2,col3) select t0.c1,t1.c2,t3.c3 from table0 t0 inner join table t1 on t1.id=t0.id inner join
table33 t3 on t3.id=t1.id where t0.date>current_date;
------
dbms.schema1.table1.col1 <-- table0.col1
dbms.schema1.table1.col2 <-- table.col2
dbms.schema1.table1.col3 <-- table33.col3
------



"""
from pglast.visitors import Visitor
from pglast import parser
import logging
import os
import pglast
from pprint import pprint as pp
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)
logger=logging.getLogger("sql_lineage_logger")


class LineageSeeker(Visitor):
    """
    Top level visitor, and start point for lineage. Here we start when we encounter SelectStmt, InsertStmt ...
    """
    def __init__(self):
        self.col_refs1=[]
        self.col_refs=Result(col_refs={})
        self.rels=Result(rels={})



    def visit_SelectStmt(self,ancestor,node):
        logger.debug("visited select stmt")
        self.col_refs.col_refs[ancestor]=[]
        self.rels.rels[ancestor]=[]
        targetClause_visitor=TargetClause_visitor(col_refs=self.col_refs,col_refs_ancestor=ancestor)

        fromClause_visitor=FromClause_visitor(rels=self.rels,rels_ancestor=ancestor)
        targetClause_visitor(node)
        fromClause_visitor(node)



class FromClause_visitor(Visitor):
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)

    def visit_RangeVar(self,ancestor,node):
        logger.debug("visited rangevar")
        self.rels.rels[self.rels_ancestor].append((node.relname,node.alias.aliasname if node.alias is not None else None))


class TargetClause_visitor(Visitor):
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)

    def visit_ResTarget(self,ancestor,node):
        logger.debug("visited res target")
        col_ref_visitor=ColumnRef_visitor(**self.__dict__)
        col_ref_visitor(node)
        

class ColumnRef_visitor(Visitor):
    def __init__(self,**kwargs) -> None:
        self.__dict__.update(kwargs)

    def visit_ColumnRef(self,ancestor,node):
        logger.debug("visited col ref")
        
        if len(node.fields)==1:
            self.col_refs.col_refs[self.col_refs_ancestor].append((None,node.fields[0].val))
        if len(node.fields)==2:
            self.col_refs.col_refs[self.col_refs_ancestor].append((node.fields[0].val,node.fields[1].val))
        
        

class Dummy:
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)


class Node:
    def __init__(self,**data):
        self.__dict__.update(data)

class SqlLineageParser:

    def get_lineage(self,sql):
        sql=parser.parse_sql(sql)
        for root in sql:
            if isinstance(root.stmt,pglast.ast.SelectStmt):
                logger.debug("DETECTED SELECT STMT")
                return self.get_graph_from_select_stmt(root.stmt)
            else:
                raise ValueError(f"{type(root)} is yet to be handled")
    
    
    def get_graph_from_select_stmt(self,root,parent='Select'):
        relations={}
        for target in root.targetList:
            if isinstance(target.val,pglast.ast.ColumnRef):
                logger.debug(f"WORKING ON COLUMN REFFERENCE {target.val}")
                node=self.create_column_node(target,parent=parent)
                if node.relation_ref not in relations.keys():
                    relations[node.relation_ref]=[node]
                else:
                    relations[node.relation_ref].append(node)
        edges=[]
        edges+=self.define_and_link_upstreams(root.fromClause,relations)
        return edges

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
                            'name':node.column_ref,
                            'parent':Node(type='table',
                                          name=getattr(relation,'relname',None),
                                          schema=getattr(relation,'schemaname',None))})
            logger.debug(f"LINKING TO UPSTREAM NODE {up_node.__dict__} WITH PARENT NODE {up_node.parent.__dict__}")
            edges.append((node,up_node))
        return edges

    def create_column_node(self,target,parent):
        col_ref=target.val
        node_data={}
        try:
            node_data.update({'name':target.name})
            logger.debug(f"TARGET NAME {target.name}")
        except:
            logger.debug(f"NO TARGET NAME")
            pass
        if len(col_ref.fields)==2:
            node_data.update({'column_ref':col_ref.fields[1].val,
                       'relation_ref':col_ref.fields[0].val})
        elif len(col_ref.fields)==1:
            node_data.update({'column_ref':col_ref.fields[0].val,'relation_ref':'*'})
        else:
            raise Exception("Node can not be defined")
        node_data['type']='column'
        node_data['parent']=parent
        if node_data.get('name') is None:
            node_data['name']=node_data['column_ref']
        
        return Node(**node_data)

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