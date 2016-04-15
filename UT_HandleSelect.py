import unittest
import database
import HandleSelect
class TestHandleSelect(unittest.TestCase):
    #db.processQuery('CREATE TABLE students (id int primary key, name varchar(20), teacherName varchar(20))') 
    #db.processQuery('CREATE TABLE teachers (id int primary key, name varchar(20))') 

    def test_From(self):
        '''
        Query without AS
        '''
        db = database.Database()
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teacher', 'exp1': 'teachername'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teacher.name']}}

        hs = HandleSelect.HandleSelect(query,db)    
        returnTable = hs.loadTable(query['from'])
        expectedTable = {'students':db.tables['students'],'teachers':db.tables['teachers'],}
        self.assertEqual( returnTable, expectedTable)

if __name__ == '__main__' and __package__ is None:
    unittest.main()
