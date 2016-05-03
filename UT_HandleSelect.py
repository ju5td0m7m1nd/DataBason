import unittest
import database
import HandleSelect
class TestHandleSelect(unittest.TestCase):
    def setUp(self):
        self.db = database.Database()
        self.hs = HandleSelect.HandleSelect(self.db,"Q")    
        if not len(self.db.tables) > 0:
            self.db.processQuery('CREATE TABLE students (id int primary key, name varchar(20), teachername varchar(20))') 
            self.db.processQuery('CREATE TABLE teachers (id int primary key, name varchar(20))')    
            self.db.processQuery('insert into students values (1, "frank", "Jason Chang")')      
            self.db.processQuery('insert into students values (3, "Su4", "Sun Hong Lai")')      
            self.db.processQuery('insert into students values (4, "Douglas", "Sun Hong Wu")')      
            self.db.processQuery("insert into teachers values (999, 'Sun Hong Wu')")      
            self.db.processQuery("insert into teachers values (888, 'Sun Hong Lai')")      
    def testFrom(self):
        
        #Query without AS
        
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teacher', 'exp1': 'teachername'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
        db = self.db
        self.hs.loadTable(query['from'])
        returnTable = self.hs.returnTables
        expectedTable = {'students':db.tables['students'],'teachers':db.tables['teachers'],}
        self.assertEqual( returnTable, expectedTable)
    
    def testFromAs(self):
        
        # Query with AS

        query = {'where': {'term2': {'operator': '>', 'exp2': 1, 'exp1': 'a.id'}, 'term1': {'operator': '=', 'exp2': 'a.teacherName', 'exp1': 'teacher.name'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'teachers'}, {'alias': 'a', 'tableName': 'students'}], 'select': {'aggFn': [], 'fieldNames': ['a.name', 'teachers.name']}}
        db = self.db
        self.hs.loadTable(query['from'])
        returnTable = self.hs.returnTables
        expectedTable = {'a':db.tables['students'],'teachers':db.tables['teachers'],}
        self.assertEqual( returnTable, expectedTable)
    '''
    def test_Where(self): 
         
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teacher', 'exp1': 'teachername'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
        hs = self.hs
        expected = {'students':{3:{'teacherName':'Sun Hong Lai','name':'su4'},4:{'teacherName':'Sun Hong Wu','name':'Douglas'}},'teachers':{888:{'name':"Sun Hong Lai" },999:{'name':"Sun Hong Wu"}}}
        returnTable = hs.checkWhere(query['where']) 
        for e in expected:
            self.assertIn(e, returnTable) 
            for columns in expected[e]:
                self.assertIn(e, returnTable)
                self.assertEqual(expected[e][columns],returnTable[e][columns])
    '''
    def test_DetermineExp(self):
        exp = "teachers.name"
        expected = {
                    999:{'tableName':'teachers','value':'Sun Hong Wu'},
                    888:{'tableName':'teachers','value':'Sun Hong Lai'}
                    }   
        self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
        returnDict = self.hs.determineExpression(exp)
        self.assertEqual(len(returnDict),len(expected))
        for key in returnDict :
            self.assertIn(key, expected)
            self.assertEqual(returnDict[key],expected[key])
    def test_DetermineExp_Alias(self):    
        # name with alias
        exp = "t.name"
        expected = {
                    999:{'tableName':'t','value':'Sun Hong Wu'},
                    888:{'tableName':'t','value':'Sun Hong Lai'}
                    }   
        self.hs.loadTable([{'alias':'t','tableName':'teachers'},{'alias':'','tableName':'students'}])
        returnDict = self.hs.determineExpression(exp)
        self.assertEqual(len(returnDict),len(expected))
        for key in returnDict :
            self.assertIn(key, expected)
            self.assertEqual(returnDict[key],expected[key])
    def test_DetermineExp_NoPrefix(self):
        # name with no prefix 
        exp = "teacherName"
        expected = {
                    1:{'tableName':'students','value':'Jason Chang'},
                    3:{'tableName':'students','value':'Sun Hong Lai'},
                    4:{'tableName':'students','value':'Sun Hong Wu'}
                    }   
        self.hs.loadTable([{'alias':'t','tableName':'teachers'},{'alias':'','tableName':'students'}])
        returnDict = self.hs.determineExpression(exp)
        self.assertEqual(len(returnDict),len(expected))
        for key in returnDict :
            self.assertIn(key, expected)
            self.assertEqual(returnDict[key],expected[key])
    def test_DetermineExp_String(self):
        exp = '"Sun Hong Wu"'
        returnString = self.hs.determineExpression(exp) 
        self.assertEqual(returnString,"Sun Hong Wu")                 
    def test_DetermineExp_Ing(self):
        exp = 1
        returnInt = self.hs.determineExpression(exp)
        self.assertEqual(returnInt, 1) 
    def test_FilterRow_twoTable(self):

        self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
        exp1 = self.hs.determineExpression('teachers.name')
        exp2 = self.hs.determineExpression('teachername')
        returnList = self.hs.filterRow(exp1,exp2,"=") 
        expected = [{'students':3,'teachers':888},{'students':4,'teachers':999}] 
        self.assertEqual(returnList,expected) 
    def test_FilterRow_String(self):

            self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
            exp1 = self.hs.determineExpression('teachers.name')
            exp2 = self.hs.determineExpression('"Sun Hong Lai"')
            returnList = self.hs.filterRow(exp1,exp2,"=") 
            expected = [{'students':1,'teachers':888},{'students':3,'teachers':888},{'students':4,'teachers':888}]
            self.assertEqual(expected,returnList) 
    def test_FilterRow_Int(self):
            self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
            exp1 = self.hs.determineExpression('teachers.id')
            exp2 = self.hs.determineExpression(900)
            returnList = self.hs.filterRow(exp1,exp2,">") 
            expected = [{'students':1,'teachers':999},{'students':3,'teachers':999},{'students':4,'teachers':999}]
            self.assertEqual(expected,returnList) 
    '''    
    From now on is 2 condition test
    '''
    def test_CheckWhere_2table(self): 
        query = {'where': {'term2': {'operator':'>','exp2':900,'exp1':'teachers.id'}, 'term1': {'operator': '=', 'exp2': 'teacherName', 'exp1': 'teachers.name'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
     
        self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
        self.hs.checkWhere(query['where']) 
        expected = [{'students':4,'teachers':999}]
        self.assertEqual(self.hs.matchPair,expected)  

    def test_CheckWhere_1table1bool_false(self): 
            query = {'where': {'term2': {'operator':'>','exp2':900,'exp1':800}, 'term1': {'operator': '=', 'exp2': 'teacherName', 'exp1': 'teachers.name'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
         
            self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
            self.hs.checkWhere(query['where']) 
            for t in self.hs.returnTables:
                self.assertEqual(self.hs.matchPair,[])
    def test_CheckWhere_1table1bool_true(self): 
                query = {'where': {'term2': {'operator':'>','exp2':700,'exp1':800}, 'term1': {'operator': '=', 'exp2': 'teacherName', 'exp1': 'teachers.name'}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
             
                self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
                self.hs.checkWhere(query['where'])
                expected = [{'students':3,'teachers':888},{'students':4,'teachers':999}] 
                self.assertEqual(self.hs.matchPair,expected)
    def test_CheckWhere_2bool_TF(self): 
                    query = {'where': {'term2': {'operator':'>','exp2':700,'exp1':800}, 'term1': {'operator': '=', 'exp2': 2, 'exp1': 1}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
                 
                    self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
                    self.hs.checkWhere(query['where'])
                    self.assertEqual(self.hs.matchPair,[])
    def test_CheckWhere_2bool_TT(self): 
                query = {'where': {'term2': {'operator':'>','exp2':700,'exp1':800}, 'term1': {'operator': '=', 'exp2': 1, 'exp1': 1}, 'logic': 'and'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
             
                self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
                self.hs.checkWhere(query['where'])
                expected = {'students':{1:{'id':1,'name':'frank','teachername':'Jason Chang'},3:{'id':3,'name':'Su4','teachername':'Sun Hong Lai'},4:{'id':4,'name':'Douglas','teachername':'Sun Hong Wu'}},'teachers':{888:{'id':888,'name':'Sun Hong Lai'},999:{'id':999,'name':'Sun Hong Wu'}}} 
                for t in self.hs.returnTables:
                    self.assertEqual(self.hs.returnTables[t].records,expected[t])
    def test_CheckWhere_2table_or(self): 
                query = {'where': {'term2': {'operator':'<','exp2':'teachers.id','exp1':900}, 'term1': {'operator': '>', 'exp2': 3, 'exp1': 'students.id'}, 'logic': 'or'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
             
                self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
                self.hs.checkWhere(query['where'])
                expected = [{'students': 4, 'teachers': 888}, {'students': 4, 'teachers': 999}, {'students': 1, 'teachers': 888}, {'students': 3, 'teachers': 888}] 
                self.assertEqual(self.hs.matchPair,expected)
    def test_CheckWhere_2bool_TF_OR(self): 
                query = {'where': {'term2': {'operator':'>','exp2':700,'exp1':800}, 'term1': {'operator': '=', 'exp2': 2, 'exp1': 1}, 'logic': 'or'}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
             
                self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
                self.hs.checkWhere(query['where'])
                expected = {'students':{1:{'id':1,'name':'frank','teachername':'Jason Chang'},3:{'id':3,'name':'Su4','teachername':'Sun Hong Lai'},4:{'id':4,'name':'Douglas','teachername':'Sun Hong Wu'}},'teachers':{888:{'id':888,'name':'Sun Hong Lai'},999:{'id':999,'name':'Sun Hong Wu'}}} 
                for t in self.hs.returnTables:
                    self.assertEqual(self.hs.returnTables[t].records,expected[t])
    '''
    From now on is 1 condition test
    '''
    def test_CheckWhere_1table(self): 
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teacherName', 'exp1': 'teachers.name'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
     
        self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
        self.hs.checkWhere(query['where']) 
        expected = [{'students':3,'teachers':888},
                    {'students':4,'teachers':999}] 
        self.assertEqual(self.hs.matchPair,expected)  

    def test_CheckWhere_1bool_F(self): 
            query = {'where': {'term2': {'operator':'>','exp2':900,'exp1':800}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
         
            self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
            self.hs.checkWhere(query['where']) 
            self.assertEqual(self.hs.matchPair,[])
    def test_CheckWhere_1bool_true(self): 
            query = {'where': {'term2': {'operator':'>','exp2':700,'exp1':800}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['name', 'teachers.name']}}
         
            self.hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])
            self.hs.checkWhere(query['where'])
            expected = [{'students':1,'teachers':888}, {'students':3,'teachers':888},{'students':4,'teachers':888},{'students':1,'teachers':999},{'students':3,'teachers':999},{'students':4,'teachers':999}] 
            for t in self.hs.returnTables:
                self.assertEqual(self.hs.matchPair,expected)
              
    def test_ExecuteQuery(self):
        query = {'where': {'term2': {'operator':'=','exp2':'teacherName','exp1':'teachers.name'}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['students.name', 'teachers.name']}}
        hs = HandleSelect.HandleSelect(self.db,query)
        hs.executeQuery() 
    def test_ExecuteQuery(self):
        query = {'where': {'term2': {'operator':'>','exp2':'students.id','exp1':1}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['students.name', 'teachers.name']}}
        hs = HandleSelect.HandleSelect(self.db,query)
        hs.executeQuery() 
    def test_ExecuteQuery_agg(self):
        query = {'where': {'term2': {'operator':'>','exp2':'students.id','exp1':1}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [{'type':'count','field':'students.id'}], 'fieldNames': []}}
        hs = HandleSelect.HandleSelect(self.db,query)
        hs.executeQuery() 


if __name__ == '__main__' and __package__ is None:
    unittest.main()
