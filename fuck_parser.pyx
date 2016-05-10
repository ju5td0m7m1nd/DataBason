from lexer import Lexer
from schema import Schema

cdef class Parser :
    '''
        Called by database.py
        process create and insert SQL command
        raise an exception on syntax error
    '''
    cdef int maxVarcharLen
    cdef Lexer lex

    def __init__(self, query):
        self.maxVarcharLen = 40
        self.lex = Lexer(query)
    
    cpdef dict parse(self) except *:
        if self.lex.matchKeyword('create'):
            return self.create()
        elif self.lex.matchKeyword('insert'):
            return self.insert()
        else:
            return self.select()

    cpdef dict select(self) except *:
        self.lex.eatKeyword('select')
        cdef dict projs = self.projectList()
        self.lex.eatKeyword('from')
        cdef list tables = self.tableList()
        cdef dict pred = {}
        if self.lex.matchKeyword('where'):
            self.lex.eatKeyword('where')
            pred = self.predicate()
#print {'select':projs, 'from':tables, 'where':pred}
        return {'select':projs, 'from':tables, 'where':pred}

    cpdef dict projectList(self) except *:
        cdef list fieldNames = []
        cdef list aggFn = []
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

    cpdef str projectField(self) except *:
        cdef str name = ''
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

    cpdef dict aggregationFn(self) except *:
        cdef str fnType = ''
        cdef str field = ''
        if self.lex.matchKeyword('count'):
            fnType = self.lex.eatKeyword('count')
        else:
            fnType = self.lex.eatKeyword('sum')
        self.lex.eatDelim('(')
        field = self.projectField()
        self.lex.eatDelim(')')
        return {'type':fnType, 'field':field}

    cpdef list tableList(self) except *:
        cdef list tList = []
        tList.append(self.tableName())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            tList.append(self.tableName())
        return tList

    cpdef dict tableName(self) except *:
        cdef str alias = ''
        cdef str name = self.lex.eatId()
        if self.lex.matchKeyword('as'):
            self.lex.eatKeyword('as')
            alias = self.lex.eatId()
        return {'tableName':name, 'alias':alias}

    cpdef dict predicate(self) except *:
        cdef str logic = ''
        cdef dict term2 = {}
        cdef dict term1 = self.term()
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

    cpdef str operator(self) except *:
        cdef str op = ''
        if self.lex.matchDelim('='):
            op = self.lex.eatDelim('=')
        elif self.lex.matchDelim('>'):
            op = self.lex.eatDelim('>')
        else:
            op = self.lex.eatDelim('<')
            if self.lex.matchDelim('>'):
                op += self.lex.eatDelim('>')
        return op

    cpdef dict create(self) except *:
        self.lex.eatKeyword('create')
        if self.lex.matchKeyword('table'):
            return self.createTable()
        elif self.lex.matchKeyword('hashindex'):
            return self.createHashIndex()
        else:
            return self.createTreeIndex()

    cpdef dict createHashIndex(self) except *:
        self.lex.eatKeyword('hashindex')
        self.lex.eatKeyword('on')
        cdef str tableName = self.lex.eatId()
        self.lex.eatDelim('(')
        cdef str attrName = self.lex.eatId()
        self.lex.eatDelim(')')
        return {'tableName': tableName, 'attr': attrName}

    cpdef dict createTreeIndex(self) except *:
        self.lex.eatKeyword('treeindex')
        self.lex.eatKeyword('on')
        cdef str tableName = self.lex.eatId()
        self.lex.eatDelim('(')
        cdef str attrName = self.lex.eatId()
        self.lex.eatDelim(')')
        return {'tableName': tableName, 'attr': attrName} 

    cpdef dict createTable(self) except *:
        self.lex.eatKeyword('table')
        tableName = self.lex.eatId()
        self.lex.eatDelim('(')
        cdef Schema schema = self.fieldDefs()
        self.lex.eatDelim(')')
        return {'tableName':tableName, 'fields':schema.fields, 'primaryKey':schema.primaryKey}

    cpdef Schema fieldDefs(self) except *:
        cdef Schema schema = self.fieldDef()
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            schema2 = self.fieldDefs()
            schema.addAll(schema2)
        return schema
    
    cpdef Schema fieldDef(self) except *:
        cdef str fieldName = self.lex.eatId()
        return self.typeDef(fieldName)

    cpdef Schema typeDef(self, name) except *:
        cdef Schema schema = Schema()
        if self.lex.matchKeyword('int'):
            self.lex.eatKeyword('int')
            if self.checkPrimaryKey():
                schema.setPrimaryKey(name)
            schema.addField(name, {'type':'int','length': 0 })
        else:
            self.lex.eatKeyword('varchar')
            self.lex.eatDelim('(')
            cdef int num = self.lex.eatNum()
            if num > self.maxVarcharLen or num <= 0:
                raise RuntimeError('Invalid varchar length.')
            self.lex.eatDelim(')')
            if self.checkPrimaryKey():
                schema.setPrimaryKey(name)
            schema.addField(name, {'type':'char', 'length': num})
        
        return schema
    
    cpdef bint checkPrimaryKey(self) except *:
        if self.lex.matchKeyword('primary'):
            self.lex.eatKeyword('primary')
            self.lex.eatKeyword('key')
            return True
        return False

    cpdef dict insert(self) except *::
        self.lex.eatKeyword('insert')
        self.lex.eatKeyword('into')
        cdef str tableName = self.lex.eatId()
        cdef list fields = []
        if self.lex.matchDelim('('):
            self.lex.eatDelim('(')
            fields = self.idList()
            self.lex.eatDelim(')')
        self.lex.eatKeyword('values')
        self.lex.eatDelim('(')
        cdef list values = self.constList()
        self.lex.eatDelim(')')
        return {'tableName':tableName, 'fields':fields, 'values':values}

    cpdef list idList(self) except *:
        cdef list arr = []
        arr.append(self.lex.eatId())
        while self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            arr.append(self.lex.eatId())
        return arr

    cpdef list constList(self) except *:
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
