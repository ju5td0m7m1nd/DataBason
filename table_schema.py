import pickle

class Table :
    
    def __init__(self,tableName, primaryKey, attributeList):
        self.tableName = tableName
        self.primaryKey = primaryKey
        self.attributeList = attributeList
        self.records = {}

    def Insert(self,newRecord):
        ErrorCode = self.CheckValid(newRecord)
        print ErrorCode
        if ErrorCode == 1 :
            self.records[len(self.records)+1] = newRecord
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

    # error code
    # 1 vaild
    # 2 unknown column
    # 3 duplicate entry for pk
    # 4 wrong value assign ( type error )
    # 5 varchar length incorrect

    def CheckValid(self,newRecord):
        self.newRecord = newRecord
        for attribute in self.attributeList:
            if not self.CheckTypeError(attribute) == True:
                return 4
           
            if attribute['name'] in self.newRecord:  
                del self.newRecord[attribute['name']]
        
        if not self.CheckUnknownColumn():
            return 2

    def CheckTypeError(self, attribute):
        columnType = attribute['type']
        columnName = attribute['name']
        if columnName in self.newRecord:    
            if columnType == 'char':
                if not type(self.newRecord[columnName]) is str:
                    return False
            elif columnType == 'int':
                if not type(self.newRecord[columnName]) is int:
                    return False
        return True
    
    def CheckUnknownColumn(self):
        if len(self.newRecord) > 0 :
            return False
        return True


    def __PrintColumn__(self):
        
        for attribute in self.attributeList:
            print attribute['name'] + ' ' + attribute['type']

t = Table('student','',[{'name':'stuname','type':'char','length':'10'},{'name':'stuid','type':'int','length':''}])
#Error 4 
#t.Insert({'stuname':'frank','stuid':'123'})

#Error 2
#t.Insert({'stuname':'frank','id':'123'})
