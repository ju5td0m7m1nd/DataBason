from lexer import Lexer
from schema import Schema

class Parser :
    '''
        Called by database.py
        process create and insert SQL command
        raise an exception on syntax error
    '''
    maxVarcharLen = 40
    
    def __init__(self, query):
        self.lex = Lexer(query)
    
    def parse(self):
        if self.lex.matchKeyword('create'):
            return self.create()
        else:
            return self.insert()

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
            if num > self.maxVarcharLen or num <= 0:
                raise RuntimeError('Invalid varchar length.')
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
#l = Parser('CREATE TABLE Student (Id INT primary key, Name VARCHAR(-10))')
'''l = Parser("insert into student (id, name) values (-2147483648, 'Mike Portnoy 123')")
d = {}
#print l.lex.tokens
try:
    d = l.parse()
    print d
except RuntimeError as e:
    print e
'''
