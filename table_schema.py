import pickle

class Table :
    
    def __init__(self,tableName, primaryKey, attributeList):
        self.tableName = tableName
        self.primaryKey = primaryKey
        self.attributeList = attributeList

        self.records = {}

    def Insert(self,Field,Value):
        if not self.CheckFieldMatch(Field,Value):
            print "Field, Value doesn't match"
            return    
        newRecord = {}
        for i in Field:
           newRecord(Field[i]) = Value[i] 
        ErrorCode = self.CheckValid(newRecord)
        if ErrorCode == 1 :
            record = self.CreateRecordObject(newRecord)
            if self.primaryKey == '':
                self.records[len(self.records)+1] = record
            else:
                pkAttr = newRecord[self.primaryKey]
                self.records[pkAttr] = record
        elif ErrorCode == 2:
            print "unknown column"
        elif ErrorCode == 3:
            print "duplicate pk"
        elif ErrorCode == 4:
            print "type error"
        elif ErrorCode == 5:
            print "varchar length incorrect"
        else:
            print "error" 

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

    def CheckValid(self,newRecord):
        self.newRecord = newRecord
        
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
        primaryAttr = self.newRecord[self.primaryKey]
        if primaryAttr in self.records:
            return False
        return True

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
            print attribute['name'] + ' ' + attribute['type']
    def __PrintData__(self):
        for r in self.records:
            print self.records[r]

#t = Table('student','stuname',{'stuname':{'type':'char','length':10},'stuid':{'type':'int','length':''}})
#Error 4 
#Error 2
#Error 3
#Error 1
#Error 5
#t.__PrintData__()
