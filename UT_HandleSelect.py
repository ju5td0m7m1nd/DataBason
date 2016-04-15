import unittest
import database
import HandleSelect
class TestHandleSelect(unittest.TestCase):
    def setUp(self):
        self.db = database.Database()
        if not len(self.db.tables) > 0:
            self.db.processQuery('CREATE TABLE students (id int primary key, name varchar(20), teacherName varchar(20))') 
            slef.db.processQuery('CREATE TABLE teachers (id int primary key, name varchar(20))')    
        
    def test_From(self):
        
        #Query without AS
        
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teacher', 'exp1': 'teachername'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
        db = self.db
        hs = HandleSelect.HandleSelect(query,db)    
        returnTable = hs.loadTable(query['from'])
        expectedTable = {'students':db.tables['students'],'teachers':db.tables['teachers'],}
        self.assertEqual( returnTable, expectedTable)
    
    def test_From_As(self):
        
        # Query with AS

        query = {'where': {'term2': {'operator': '>', 'exp2': 1, 'exp1': 'a.id'}, 'term1': {'operator': '=', 'exp2': 'a.teacherName', 'exp1': 'teacher.name'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'teachers'}, {'alias': 'a', 'tableName': 'students'}], 'select': {'aggFn': [], 'fieldNames': ['a.name', 'teachers.name']}}
        db = self.db
        hs = HandleSelect.HandleSelect(query,db)    
        returnTable = hs.loadTable(query['from'])
        expectedTable = {'a':db.tables['students'],'teachers':db.tables['teachers'],}
        self.assertEqual( returnTable, expectedTable)

    def test_Where(self): 

if __name__ == '__main__' and __package__ is None:
    unittest.main()
