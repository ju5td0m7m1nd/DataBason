import glob
import pickle
import re
from parser import Parser
from table_schema import *
from HandleSelect import *
from btree import *

class Database:
    '''
        tables -- a dictionary. key:tableName, value:a Table
        
    '''
    file_dir = 'files/'
    command = ''
    def __init__(self):
        self.tables = {}
        self.tree_indexes = {}
        self.hash_indexes = {}
        self.toBeSaved = []
        # load all the tables from the disk (temporary implementation)
        tablePaths = glob.glob(self.file_dir + '*.pkl')
        for p in tablePaths:
            table = self.loadTable(p)
            self.tables[table.tableName] = table

    def processQuery(self, s):
        parser = Parser(s)
        cmd = re.split('\s+',s.strip(),1)[0].lower()
        if cmd=='create':
            cmdType = re.split('\s+', s.strip(),2)[1].lower()
            self.command = cmd + ' ' + cmdType
            if cmdType=='table':
                data = parser.parse()
                table = Table(data['tableName'], data['primaryKey'], data['fields'])
                self.addTable(table)
                #print 'created table: ', data
                return table
            elif cmdType=='treeindex':
                data = parser.parse()
                index = self.tables[data['tableName']].treeIndex(data['attr'])
                self.addIndex(data['tableName'], data['attr'], index, 'tree')
            elif cmdType=='hashindex':
                data = parser.parse()
                index = self.tables[data['tableName']].hashIndex(data['attr'])
                self.addIndex(data['tableName'], data['attr'], index, 'hash')

        elif cmd=='insert':
            self.command = 'insert'
            data = parser.parse()
            if data['tableName'] not in self.tables:
                raise RuntimeError('Unkown table: ' + data['tableName'])
            table = self.tables[data['tableName']]
            table.Insert(data['fields'], data['values'])
            self.toBeSaved.append(table)
            #self.saveTable(table, table.tableName)
            #print 'cur table: ', table.records
            return table
        elif cmd=='select':
            self.command = 'select'
            data = parser.parse()
            # use HandleSelect class to validate the select data
            hs = HandleSelect(self,data)
            hs.executeQuery()
            #print hs.selectResult
            return hs.selectResult
        else:
            raise RuntimeError('Unkown keyword: ' + cmd)
    
    def addIndex(self, tableName, attr ,newIndex, idxType):
        # the key of the index dict is tableName#attrName
        if idxType == 'hash':
            self.hash_indexes[tableName+'#'+attr] = newIndex
        else:
            self.tree_indexes[tableName+'#'+attr] = newIndex
        # add save index

    def addTable(self, newTable):
        if newTable.tableName in self.tables:
            raise RuntimeError('Duplicated table name: ' + newTable.tableName)
        self.tables[newTable.tableName] = newTable
        self.saveTable(newTable, newTable.tableName)

    def loadTable(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    def saveTable(self, obj, name):
        with open(self.file_dir + name + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    def saveAll(self):
        already_saved = []
        while self.toBeSaved:
            element = self.toBeSaved.pop()
            if element.tableName not in already_saved:
                already_saved.append(element.tableName)
                self.saveTable(element, element.tableName)

## test
if __name__ == '__main__':
    print "NO"
    db = Database()
    s ="CREATE TABLE Item (id int primary key, des varchar(20), a_field int)" 
    s1 = "create treeindex on Item(des)"
    s2 = "insert into Item values (8, 'a', 100)"
    s3 = "insert into Item values (9, 'b', 200)"
    s4 = "insert into Item values (10, 'c', 300)"
    s5 = "insert into Item values (11, 'd', 400)"
    db.processQuery(s)
    db.processQuery(s2)
    db.processQuery(s3)
    db.processQuery(s4)
    db.processQuery(s5)
    db.processQuery(s1)
    print db.tree_indexes
    #select = "select name, teacher.name from students, teachers where teacherName=teacher.name"
    # db.processQuery(select)
