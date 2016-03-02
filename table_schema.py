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

    def CheckVaild(self,record):

        for attribute in self.attributeList:
            print type(record[attribute['name']])
            if attribute['type'] == 'char':
                if not type(record[attribute['name']]) is str :
                    return 4
            elif attribute['type'] == 'int':
                if not type(record[attribue['name']]) is int :
                    return 4

            return 1
            #check type
            #if type(record[attribute]) != type(attributeType):
            #    return 4

            #if
         
