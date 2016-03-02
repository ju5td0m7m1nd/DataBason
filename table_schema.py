import pickle

class Table :
    
    def __init__(self,tableName, primaryKey, attributeList):
        self.tableName = tableName
        self.primaryKey = primaryKey
        self.attributeList = attributeList
        self.records = {}

    def Insert(self,newRecord):
        print newRecord
        if self.CheckVaild(newRecord) == 1 :
            self.records[len(self.records)+1] = newRecord
        else:

            print "error" 

    # error code
    # 1 vaild
    # 2 unknown column
    # 3 duplicate entry for pk
    # 4 wrong value assign ( type error )
    # 5 varchar length incorrect

    def CheckValid(self,record):

        for attribute in self.attributeList:
                
            columnType = attribute['type']
            columnName = attribute['name']
            print columnType
            print columnName
            print record[columnName]
            if columnType == 'char':
                if not type(record[attribute['name']]) is str :
                    return 4
            elif columnType == 'int':
                if not type(record[attribute['name']]) is int :
                    return 4
            #check type
            #if type(record[attribute]) != type(attributeType):
            #    return 4

            #if
        return 1         

    def __PrintColumn__(self):
        
        for attribute in self.attributeList:
            print attribute['name'] + ' ' + attribute['type']
