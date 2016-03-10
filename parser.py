import pickle
from lexer import Lexer
from schema import Schema

class Parser :
   
    def __init__(self, query):
        self.lex = Lexer(query)
    
    def updateCmd(self):
        if self.lex.matchKeyword('create'):
            return self.create()
        elif self.lex.matchKeyword('insert'):
            return self.insert()
        else:
            raise RuntimeError('Unsupported SQL query.')

    def create(self):
        self.lex.eatKeyword('create')
        if self.lex.matchKeyword('table'):
            return self.createTable()
   
    def createTable(self):
        self.lex.eatKeyword('table')
        tableName = self.lex.eatId()
        self.lex.eatDelim('(')
        schema = self.fieldDefs()
        self.lex.eatDelim(')')
        return {'tableName':tableName, 'fields':schema.fields, 'primaryKey':schema.primaryKey}

    def fieldDefs(self):
        schema = self.fieldDef()
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            schema2 = self.fieldDefs()
            schema.addAll(schema2)
        return schema
    
    def fieldDef(self):
        fieldName = self.lex.eatId()
        return self.typeDef(fieldName)

    def typeDef(self, name):
        schema = Schema()
        if self.lex.matchKeyword('int'):
            self.lex.eatKeyword('int')
            if self.checkPrimaryKey():
                schema.setPrimaryKey(name)
            schema.addField(name, {'type':'int','length': 0 })
        else:
            self.lex.eatKeyword('varchar')
            self.lex.eatDelim('(')
            num = self.lex.eatNum()
            self.lex.eatDelim(')')
            if self.checkPrimaryKey():
                schema.setPrimaryKey(name)
            schema.addField(name, {'type':'char', 'length': num})
        
        return schema
    
    def checkPrimaryKey(self):
        if self.lex.matchKeyword('primary'):
            self.lex.eatKeyword('primary')
            self.lex.eatKeyword('key')
            return True
        return False

    def insert(self):
        self.lex.eatKeyword('insert')
        self.lex.eatKeyword('into')
        tableName = self.lex.eatId()
        fields = []
        if self.lex.matchDelim('('):
            self.lex.eatDelim('(')
            fields = self.idList()
            self.lex.eatDelim(')')
        self.lex.eatKeyword('values')
        self.lex.eatDelim('(')
        values = self.constList()
        self.lex.eatDelim(')')
        return {'tableName':tableName, 'fields':fields, 'values':values}

    def idList(self):
        arr = []
        arr.append(self.lex.eatId())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            arr.append(self.lex.eatId())
        return arr

    def constList(self):
        arr = []
        arr.append(self.const())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            arr.append(self.const())
        return arr
    
    def const(self):
        if self.lex.matchNum():
            return self.lex.eatNum()
        else:
            return self.lex.eatVarchar()

    

# test
#l = Parser('CREATE TABLE Student (Id INT primary key, Name VARCHAR(10))')
l = Parser("insert into student (id, name) values (123, 'Mike')")
d = l.updateCmd()
print d
