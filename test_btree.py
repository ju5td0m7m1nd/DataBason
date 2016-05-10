import glob
import pickle
import re
from parser import Parser
from table_schema import Table
from btree import *

class Database:
    file_dir = 'files/'
    command = ''
    def __init__(self):
        self.tables = {} 
        # load all the tables from the disk (temporary implementation)
        '''
        tablePaths = glob.glob(self.file_dir + '*.pkl')
        for p in tablePaths:
            table = self.loadTable(p)
            self.tables[table.tableName] = table
        '''
        l = range(100)
        bt = BPlusTree(20)
        for item in l:
            bt.insert(item, str(item))
        
        print bt
        
        self.saveTable(bt, 'btree')
        paths = glob.glob(self.file_dir + '*.pkl')
        for p in paths:
            t = self.loadTable(p)
            print t
    
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
## test
db = Database()
