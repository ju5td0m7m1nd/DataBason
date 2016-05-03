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
        elif self.lex.matchKeyword('insert'):
            return self.insert()
        else:
            return self.select()

    def select(self):
        self.lex.eatKeyword('select')
        projs = self.projectList()
        self.lex.eatKeyword('from')
        tables = self.tableList()
        pred = {}
        if self.lex.matchKeyword('where'):
            self.lex.eatKeyword('where')
            pred = self.predicate()
#print {'select':projs, 'from':tables, 'where':pred}
        return {'select':projs, 'from':tables, 'where':pred}

    def projectList(self):
        fieldNames = []
        aggFn = []
        if self.lex.matchId() or self.lex.matchDelim('*'):
            fieldNames.append(self.projectField())
        else:
            aggFn.append(self.aggregationFn())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            if self.lex.matchId():
                fieldNames.append(self.projectField())
            else:
                aggFn.append(self.aggregationFn())
        return {'fieldNames':fieldNames, 'aggFn':aggFn}

    def projectField(self):
        name = ''
        if self.lex.matchDelim('*'):
            name = self.lex.eatDelim('*')
        else:
            name = self.lex.eatId()
            if self.lex.matchDelim('.'):
                name += self.lex.eatDelim('.')
                if self.lex.matchDelim('*'):
                    name += self.lex.eatDelim('*')
                else:
                    name += self.lex.eatId()
        return name

    def aggregationFn(self):
        fnType = ''
        field = ''
        if self.lex.matchKeyword('count'):
            fnType = self.lex.eatKeyword('count')
        else:
            fnType = self.lex.eatKeyword('sum')
        self.lex.eatDelim('(')
        field = self.projectField()
        self.lex.eatDelim(')')
        return {'type':fnType, 'field':field}

    def tableList(self):
        tList = []
        tList.append(self.tableName())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            tList.append(self.tableName())
        return tList

    def tableName(self):
        alias = ''
        name = self.lex.eatId()
        if self.lex.matchKeyword('as'):
            self.lex.eatKeyword('as')
            alias = self.lex.eatId()
        return {'tableName':name, 'alias':alias}

    def predicate(self):
        logic = ''
        term2 = {}
        term1 = self.term()
        if self.lex.matchKeyword('and'):
            logic = self.lex.eatKeyword('and')
            term2 = self.term()
        elif self.lex.matchKeyword('or'):
            logic = self.lex.eatKeyword('or')
            term2 = self.term()
        return {'term1':term1, 'term2':term2, 'logic':logic}

    def term(self):
        exp1 = self.expression()
        op = self.operator()
        exp2 = self.expression()
        return {'exp1': exp1, 'exp2': exp2, 'operator': op}
    
    def expression(self):
        if self.lex.matchId():
            return self.projectField()
        elif self.lex.matchNum():
            return self.lex.eatNum()
        else:
            return '"'+self.lex.eatVarchar()+'"'

    def operator(self):
        op = ''
        if self.lex.matchDelim('='):
            op = self.lex.eatDelim('=')
        elif self.lex.matchDelim('>'):
            op = self.lex.eatDelim('>')
        else:
            op = self.lex.eatDelim('<')
            if self.lex.matchDelim('>'):
                op += self.lex.eatDelim('>')
        return op

    def create(self):
        self.lex.eatKeyword('create')
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
#l = Parser("insert into student (id, name) values (-2147483648, 'Mike Portnoy 123')")
#d = {}
#l = Parser("select count(NAME) from AUTHOR where NATIONALITY = 'Taiwan'")
#l = Parser("select * from students where id < 5")
#l.parse()
#print l.lex.tokens
'''try:
    d = l.parse()
    print d
except RuntimeError as e:
    print e
'''
