import glob
import pickle
import re
from parser import Parser
from table_schema import Table

class Database:
    '''
        tables -- a dictionary. key:tableName, value:a Table
        
    '''
    file_dir = 'files/'
    def __init__(self):
        self.tables = {} 
        # load all the tables from the disk (temporary implementation)
        tablePaths = glob.glob(self.file_dir + '*.pkl')
        for p in tablePaths:
            table = self.loadTable(p)
            self.tables[table.tableName] = table

    def processQuery(self, s):
        parser = Parser(s)
        cmd = re.split('\s+',s.strip(),1)[0].lower()
        if cmd=='create':
            data = parser.parse()
            table = Table(data['tableName'], data['primaryKey'], data['fields'])
            self.addTable(table)
            print 'created table: ', data
        elif cmd=='insert':
            data = parser.parse()
            if data['tableName'] not in self.tables:
                raise RuntimeError('Unkown table. ')
            table = self.tables[data['tableName']]
            table.Insert(data['fields'], data['values'])
            self.saveTable(table, table.tableName)
            print 'cur table: ', table.records
        else:
            raise RuntimeError('Unkown keywords.')

    def addTable(self, newTable):
        if newTable.tableName in self.tables:
            raise RuntimeError('Duplicated table name.')
        self.tables[newTable.tableName] = newTable
        self.saveTable(newTable, newTable.tableName)

    def loadTable(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    def saveTable(self, obj, name):
        with open(self.file_dir + name + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
## test
db = Database()
s = 'CREATE TABLE Student (Id INT primary key, Name VARCHAR(10))'
s2 = "insert into student (id, name) values (-2147483648, 'Mike')"
db.processQuery(s)
db.processQuery(s2)
