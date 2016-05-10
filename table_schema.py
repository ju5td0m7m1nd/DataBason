from collections import OrderedDict
from HandleSelect import *
from btree import *
class Table :
    '''
        After parsing the SQL string, use this class to create a table structure in memory.
        The parameters passed into __init__ has been verified by Parser.
        The members of the class:
            tableName : The name of the table
            primaryKey : Attribute name of the primary key; '' if not assigned
            attributeList: A dictionary storing the schema
                key: attr name
                value: {type->(int/char), length->(number)}
            records: A dictionary storing all the tuples in the table. 
                key: primary key, auto-incremented number if no primary key 
                value: a field->value dictionary
    '''
    def __init__(self,tableName, primaryKey, attributeList):
        self.tableName = tableName
        self.primaryKey = primaryKey
        self.attributeList = attributeList
        self.records = {}

    def treeIndex(self, attr, order=125):
        bt = BPlusTree(order)
        for v in self.records.values():
            bt.insert(v[attr], v)
        print bt
        return bt

    def hashIndex(self, attr):
        
        return {}

    def Insert(self,Field,Value):
        newRecord = {} 
        if not Field: 
            if len(Value) == len(self.attributeList): 
                for f in self.attributeList:
                    newRecord[f] = Value[self.attributeList.keys().index(f)]
            else:
                raise RuntimeError('Wrong number of value.')
        else:    
            if not self.CheckFieldMatch(Field,Value):
                print("Field, Value doesn't match")
                raise RuntimeError("Field, value doesn't match.")
            for f in Field:
                if f in newRecord:
                    raise RuntimeError('Repeated field name: '+f)
                newRecord[f] = Value[Field.index(f)]
            
        ErrorCode = self.CheckValid(newRecord)
        if ErrorCode == 1 :
            record = self.CreateRecordObject(newRecord)
            if self.primaryKey == '':
                self.records[len(self.records)+1] = record
            else:
                pkAttr = newRecord[self.primaryKey]
                self.records[pkAttr] = record
        elif ErrorCode == 2:
            print ("unknown column")
            raise RuntimeError("unknown column")
        elif ErrorCode == 3:
            print ("duplicated primary key")
            raise RuntimeError("duplicated primary key")
        elif ErrorCode == 4:
            print ("type error")
            raise RuntimeError("type error")
        elif ErrorCode == 5:
            print ("varchar length incorrect")
            raise RuntimeError("varchar length incorrect")
        elif ErrorCode == 6:
            print ("missing primary key")
            raise RuntimeError("missing primary key")
        else:
            print ("error")

    def CreateRecordObject(self,newRecord):
        r = {}
        for attr in self.attributeList:
            r[attr] = None
        for attr in newRecord:
            r[attr] = newRecord[attr]
        return r
    def CheckFieldMatch(self,Field,Value):
        if not len(Field) == len(Value):
            return False
        return True

    # error code
    # 1 vaild
    # 2 unknown column
    # 3 duplicate entry for pk
    # 4 wrong value assign ( type error )
    # 5 varchar length incorrect
    # 6 missing primary key
    def CheckValid(self,newRecord):
        self.newRecord = newRecord
        
        if self.MissingPrimaryKey():
            return 6
        if not self.CheckPrimaryKey():
            return 3
        for r in self.newRecord:
            if not self.CheckUnknownColumn(r) :
                return 2  
            if not self.CheckTypeError(r) :
                return 4
            if not self.CheckVarLength(r) :
                return 5 
        return 1
    def CheckPrimaryKey(self):
        if self.primaryKey:
            primaryAttr = self.newRecord[self.primaryKey]
            if primaryAttr in self.records:
                return False
        return True
    def MissingPrimaryKey(self):
        if self.primaryKey:
            if not self.primaryKey in self.newRecord:
                return True
        return False
    def CheckTypeError(self, attribute):
        columnType = self.attributeList[attribute]['type']
        if columnType == 'char':
            if not type(self.newRecord[attribute]) is str:
                return False
        elif columnType == 'int':
            if not type(self.newRecord[attribute]) is int:
                return False
        return True
   
    def CheckVarLength(self,attribute):
        if type(self.newRecord[attribute]) is str:
            if len(self.newRecord[attribute]) > self.attributeList[attribute]['length']:
                return False
        return True
 
    def CheckUnknownColumn(self,attribute):
        if not attribute in self.attributeList:
            return False
        return True


    def __PrintColumn__(self):
        
        for attribute in self.attributeList:
            print (attribute['name'] + ' ' + attribute['type'])
    def __PrintData__(self):
        for r in self.records:
            print (self.records[r])

