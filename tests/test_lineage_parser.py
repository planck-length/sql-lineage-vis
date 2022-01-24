import unittest
from app.sql_lineage_parser import SqlLineageParser
from pprint import pprint as pp
import pytest

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
    
    def test_get_lineage_from_select_no_table_alias_with_joins(self):
        sql="""
        SELECT col1,col2 from table1 inner join table2 on table1.id=table2.id;
        """
        expected=[('None.table1.col1','Select.col1'),('None.table1.col2','Select.col2'),
        ('None.table2.col1','Select.col1'),('None.table2.col2','Select.col2')]
        
        lineage_parser=SqlLineageParser()
        val=lineage_parser.get_lineage(sql)
        val_ref=lineage_parser.normalize_for_nx_graph(val)
        self.assertListEqual(sorted(val_ref),sorted(expected))
    

    def test_get_update_stmt_basic(self):
        sql="""
        UPDATE test_schema.test_table SET 
        id=t.col1,
        f_name=t.col2,
        l_name=t.col3,
        created_date=t.col4
        FROM (SELECT col1,col2,col3,col4 FROM table1)t;
        """
        expected=[('None.table1.col1','SUBQUERY.t.col1'),
        ('None.table1.col2','SUBQUERY.t.col2'),
        ('None.table1.col3','SUBQUERY.t.col3'),
        ('None.table1.col4','SUBQUERY.t.col4'),
        ('SUBQUERY.t.col1','test_schema.test_table.id'),
        ('SUBQUERY.t.col2','test_schema.test_table.f_name'),
        ('SUBQUERY.t.col3','test_schema.test_table.l_name'),
        ('SUBQUERY.t.col4','test_schema.test_table.created_date')]

        


def lineage_tester(sql,expected):
    lineage_parser=SqlLineageParser()
    val=lineage_parser.get_lineage(sql)
    val_ref=lineage_parser.normalize_for_nx_graph(val)
    assert sorted(val_ref)==sorted(expected)


def update_stmt_cases():
    sql_1="""
    UPDATE test_schema.test_table
    SET 
     id=1,
     name='Foo',
     c_date='2020-01-01'
    WHERE 1=1;
    """
    expected_1=[
    ("Constant.1","test_schema.test_table.id"),
    ("Constant.Foo","test_schema.test_table.name"),
    ("Constant.2020-01-01","test_schema.test_table.c_date")]

    sql_2="""
    UPDATE test_schema.test_table SET 
    id=t.col1,
    f_name=t.col2,
    l_name=t.col3,
    created_date=t.col4
    FROM (SELECT col1,col2,col3,col4 FROM table1)t;
    """
    expected_2=[('None.table1.col1','SUBQUERY.t.col1'),
    ('None.table1.col2','SUBQUERY.t.col2'),
    ('None.table1.col3','SUBQUERY.t.col3'),
    ('None.table1.col4','SUBQUERY.t.col4'),
    ('SUBQUERY.t.col1','test_schema.test_table.id'),
    ('SUBQUERY.t.col2','test_schema.test_table.f_name'),
    ('SUBQUERY.t.col3','test_schema.test_table.l_name'),
    ('SUBQUERY.t.col4','test_schema.test_table.created_date')]

    test_cases=[(sql_1,expected_1),(sql_2,expected_2)]
    return test_cases
    

def create_stmt_cases():
    sql1="""
    CREATE TABLE test_schema.test_table AS
    SELECT id,name as full_name,date created_date FROM up_schema.up_table
    WHERE date>current_date-100;
    """
    expected_1=[("up_schema.up_table.id","test_schema.test_table.id"),
                ("up_schema.up_table.name","test_schema.test_table.full_name"),
                ("up_schema.up_table.date","test_schema.test_table.created_date")]

    
    sql2="""
    CREATE TABLE test_schema.test_table (id integer,name varchar);
    """
    expected_2=NotImplementedError()

    sql3="""
    SELECT u.id,u.mail as e_mail,COUNT(o.id) as number_of_orders INTO temp.order_count FROM
     users u inner join sales.orders o on o.user_id=u.id
     GROUP by u.id,u.mail
    """
    expected_3=[("None.users.id","temp.order_count.id"),
                ("None.users.mail","temp.order_count.e_mail"),
                ("sales.orders.id","temp.order_count.number_of_orders")]

    test_cases=[(sql1,expected_1),(sql2,expected_2),(sql3,expected_3)]
    return test_cases

def select_stmt_cases():
    sql_1="""
    SELECT sub.* FROM (SELECT id,name full_name,date created_date FROM test_schema.test_table) sub;
    """
    expected_1=[("test_schema.test_table.id","SUBQUERY.sub.id"),
                ("test_schema.test_table.name","SUBQUERY.sub.full_name"),
                ("test_schema.test_table.date","SUBQUERY.sub.created_date"),
                ("SUBQUERY.sub.*","Select.*")]
    
    return [(sql_1,expected_1)]

@pytest.mark.parametrize("sql,expected",update_stmt_cases(),ids=["with_values","with_select"])
def test_get_lineage_from_update_stmt(sql,expected):
    lineage_tester(sql,expected)

@pytest.mark.parametrize("sql,expected",create_stmt_cases(),ids=["create_with_as","create_without_as","select_into"])
def test_get_lineage_from_create_stmt(sql,expected):
    if isinstance(expected,Exception):
        with pytest.raises(NotImplementedError) as exc_info:
            lineage_tester(sql,expected)
 
        assert str(exc_info._excinfo[1]).endswith("yet to be handled")==True
        return
    lineage_tester(sql,expected)

@pytest.mark.parametrize("sql,expected",select_stmt_cases(),ids=["select_star"])
def test_get_lineage_from_select_stmt(sql,expected):
    lineage_tester(sql,expected)
