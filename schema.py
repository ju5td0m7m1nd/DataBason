import re
from collections import OrderedDict
class Schema:
    
    def __init__(self):
        self.fields = OrderedDict()
        self.primaryKey = ''
    
    def setPrimaryKey(self, fieldName):
        self.primaryKey = fieldName

    def addField(self, fieldName, fieldType):
        if fieldName in self.fields:
            raise RuntimeError('Duplicated field name.')
        else:
            self.fields[fieldName] = fieldType
    
    def addAll(self, sch):
        for f in sch.fields:
            self.addField(f, sch.fields[f])
        if self.primaryKey and sch.primaryKey:
            raise RuntimeError('Multiple primary keys.')
        elif sch.primaryKey:
            self.setPrimaryKey(sch.primaryKey)

