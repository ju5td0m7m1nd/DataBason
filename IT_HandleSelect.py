import unittest
import database
import HandleSelect
class TestHandleSelect(unittest.TestCase):
    def setUp(self):
        self.db = database.Database()

    '''
    Name rule test_(conditionNum)_(tableNum)_(aggBool)
    '''
    def test_0_2_0(self):
        query = {'where': {'term2': {}, 'term1': {}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['students.name', 'teachers.name']}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        print("no where condition")
        self.hs.executeQuery()
    def test_1_2_0(self):
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teachers.name', 'exp1': 'teachername'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['teachername', 'teachers.id']}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()
    def test_1_2_0_star(self):
        query = {'where': {'term2': {}, 'term1': {'operator': '=', 'exp2': 'teachers.name', 'exp1': 'teachername'}, 'logic': ''}, 'from': [{'alias': '', 'tableName': 'students'}, {'alias': '', 'tableName': 'teachers'}], 'select': {'aggFn': [], 'fieldNames': ['students.*', 'teachers.*']}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()
    def test_1_1_0_star(self):
        query = {'where': {'term2': {}, 'term1': {'operator': '>', 'exp2': 0, 'exp1': 'students.id'}, 'logic': ''}, 'from': [ {'alias':'','tableName': 'students'}],'select': {'aggFn': [], 'fieldNames': ['students.*']}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()
    def test_1_1_0(self):
        query = {'where': {'term2': {}, 'term1': {'operator': '<', 'exp2': 3, 'exp1': 'students.id'}, 'logic': ''}, 'from': [ {'alias':'','tableName': 'students'}],'select': {'aggFn': [], 'fieldNames': ['students.name', 'students.teachername']}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()
    def test_1_1_1(self):
        query = {'where': {'term2': {}, 'term1': {'operator': '<', 'exp2': 3, 'exp1': 'students.id'}, 'logic': ''}, 'from': [ {'alias':'','tableName': 'students'}],'select': {'aggFn': [{'type':'count','field':'students.id'}], 'fieldNames': []}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()
    def test_1_1_0(self):
        query = {'where': {}, 'logic': ''}, 'from': [ {'alias':'','tableName': 'students'}],'select': {'aggFn': [{'type':'count','field':'students.id'}], 'fieldNames': []}}
        self.hs = HandleSelect.HandleSelect(self.db,query)
        self.hs.executeQuery()



if __name__ == '__main__' and __package__ is None:
    unittest.main()
