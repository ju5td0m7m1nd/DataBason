import pickle
from lexer import Lexer
from createTableData import CreateTableData

class Parser :
   
    def __init__(self, query):
        self.lex = Lexer(query)
    
    def updateCmd(self):
        if self.lex.matchKeyword('create'):
            return self.create()
        elif self.lex.machKeyword('insert'):
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
        return CreateTableData(tableName, schema)

    def fieldDefs(self):
        schema = self.fieldDef()
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            schema2 = self.fieldDefs()
            schema.addAll(shcema2)
        return schema
    
    def fieldDef(self):
        fieldName = self.lex.eatId()
        return self.typeDef(fieldName)

    def typeDef(self, name):
        schema = Schema()
        if self.lex.matchKeyword('int'):
            self.lex.eatKeyword('int')
            schema.addField(name, 'int')
        else:
            self.lex.eatKeyword('varchar')
            self.lex.eatDelim('(')
            num = self.lex.eatNum()
            self.lex.eatDelim(')')
            shema.addField(name, '')

