import unittest
from app.sql_lineage_parser import SqlLineageParser
from pprint import pprint as pp
class TestLineageParser(unittest.TestCase):

    def test_get_lineage(self):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT t.col1,t.col2 FROM table1 t")
        expected=[('None.table1.col1','Select.col1'),
                  ('None.table1.col2','Select.col2')]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)
    
    def test_get_lineage_column_alias(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT t.col1 as column1 ,t.col2 column2 FROM table1 t")
        expected=[('None.table1.col1','Select.column1'),
                  ('None.table1.col2','Select.column2')]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)       

    def test_get_lineage_column_relation_no_alias(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT col1 as column1 ,col2 column2 FROM table1")
        expected=[('None.table1.col1','Select.column1'),
                  ('None.table1.col2','Select.column2')]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)       

    def test_get_lineage_column_relation_no_subq(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT col1 as column1 ,col2 column2 FROM table1")
        expected=[('None.table1.col1','Select.column1'),
                  ('None.table1.col2','Select.column2')]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)       


    def test_get_lineage_with_subquery(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT sub.col1 as column1 ,sub.col2 column2 FROM (SELECT col1,col2 FROM table1) sub")
        expected=[('SUBQUERY.sub.col1','Select.column1'),
                  ('SUBQUERY.sub.col2','Select.column2'),
                  ('None.table1.col1','SUBQUERY.sub.col1'),
                  ('None.table1.col2','SUBQUERY.sub.col2')]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)  

    def test_get_lineage_with_nested_subquery(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT sub.col1 as column1 ,sub.col2 column2 FROM (SELECT sub2.col1,sub2.col2 FROM (SELECT col1,col2 FROM table1)sub2) sub")
        expected=[('SUBQUERY.sub.col1','Select.column1',),
                  ('SUBQUERY.sub.col2','Select.column2',),
                  ('SUBQUERY.sub2.col1','SUBQUERY.sub.col1',),
                  ('SUBQUERY.sub2.col2','SUBQUERY.sub.col2',),
                  ('None.table1.col1','SUBQUERY.sub2.col1',),
                  ('None.table1.col2','SUBQUERY.sub2.col2',)]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)  

    def test_get_lineage_with_table_join_table(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT t1.col1 as column1 ,t2.col2 column2 FROM table1 t1 INNER JOIN table2 t2 on t2.id=t1.id")
        expected=[('None.table1.col1','Select.column1',),
                  ('None.table2.col2','Select.column2',)]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)  

    def test_get_lineage_with_table_join_subquery(self):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT t1.col1 as column1 ,t2.col2 column2 FROM table1 t1 INNER JOIN (SELECT col2 FROM table2) t2 on t2.id=t1.id")
        expected=[('None.table1.col1','Select.column1',),
                  ('SUBQUERY.t2.col2','Select.column2',),
                  ('None.table2.col2','SUBQUERY.t2.col2',)]
        val_ref=[]
        for edge in val:
            up,do = edge
            up_schema=str(up.parent.schema)+'.' if not isinstance(up.parent,str) else ''
            do_schema=str(do.parent.schema)+'.' if not isinstance(do.parent,str) else ''
            ups=f"{up_schema}{up.parent if isinstance(up.parent,str) else up.parent.name}.{up.name}"
            dos=f"{do_schema}{do.parent if isinstance(do.parent,str) else do.parent.name}.{do.name}"
            val_ref.append((ups,dos))

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)

    def test_get_lineage_with_table_join_subquery_nested(self,):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("SELECT t1.col1 as column1 ,t2.col2 column2,t2.col3 FROM table1 t1 INNER JOIN (SELECT t2.col2,t33.col3 FROM table2 t2 inner join table33 t33 on t33.id=t2.id) t2 on t2.id=t1.id")
        expected=[('None.table1.col1','Select.column1',),
                  ('SUBQUERY.t2.col2','Select.column2',),
                  ('SUBQUERY.t2.col3','Select.col3',),
                  ('None.table2.col2','SUBQUERY.t2.col2',),
                  ('None.table33.col3','SUBQUERY.t2.col3',)]
        val_ref=lineage_parser.normalize_for_nx_graph(val)

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(val_ref,expected)

    def test_get_lineage_from_insert_stmt_select(self):
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage("INSERT INTO test_schema.test_table (first_col,second_col,third_col) SELECT t.id,t.name as first_name,t.date FROM table1 t")
        expected=[('test_schema.test_table.id','test_schema.test_table.first_col',),
                  ('test_schema.test_table.first_name','test_schema.test_table.second_col'),
                  ('test_schema.test_table.date','test_schema.test_table.third_col',),
                  ('None.table1.id','test_schema.test_table.id',),
                  ('None.table1.name','test_schema.test_table.first_name',),
                  ('None.table1.date','test_schema.test_table.date',)]

        val_ref=lineage_parser.normalize_for_nx_graph(val)

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(sorted(val_ref),sorted(expected))
    
    def test_get_lineage_from_two_stmts(self):
        lineage_parser=SqlLineageParser()
        sql="""
        INSERT INTO test_schema.test_table (first_col,second_col,third_col) SELECT t.id,t.name as first_name,t.date FROM table1 t;
        SELECT t.first_col as id FROM test_schema.test_table t;
        """
        val=lineage_parser.get_lineage(sql)
        expected=[('test_schema.test_table.id','test_schema.test_table.first_col',),
                  ('test_schema.test_table.first_name','test_schema.test_table.second_col'),
                  ('test_schema.test_table.date','test_schema.test_table.third_col',),
                  ('None.table1.id','test_schema.test_table.id',),
                  ('None.table1.name','test_schema.test_table.first_name',),
                  ('None.table1.date','test_schema.test_table.date',),
                  ('test_schema.test_table.first_col','Select.id')]

        val_ref=lineage_parser.normalize_for_nx_graph(val)

        #pp(expected)
        #pp(val_ref)
        self.assertListEqual(sorted(val_ref),sorted(expected))
    
    def test_get_lineage_from_insert_with_values(self):
        sql="""
        INSERT INTO test_schema.test_table (col1,col2,col3,col4) VALUES (1,'f_name','email@host.com','2020-01-01');
        """
        expected=[('Constant.1','test_schema.test_table.col1'),
            ('Constant.f_name','test_schema.test_table.col2'),
            ('Constant.email@host.com','test_schema.test_table.col3'),
            ('Constant.2020-01-01','test_schema.test_table.col4')]
        lineage_parser=SqlLineageParser()
        
        val=lineage_parser.get_lineage(sql)
        val_ref=lineage_parser.normalize_for_nx_graph(val)
        self.assertListEqual(sorted(val_ref),sorted(expected))
    
    def test_get_lineage_from_select_no_table_alias(self):
        sql="""
        SELECT col1,col2 from table1;
        """
        expected=[('None.table1.col1','Select.col1'),('None.table1.col2','Select.col2')]
        
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage(sql)
        val_ref=lineage_parser.normalize_for_nx_graph(val)
        self.assertListEqual(sorted(val_ref),sorted(expected))