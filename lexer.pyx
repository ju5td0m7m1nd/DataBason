import re
import shlex

cdef class Lexer:
    cdef list tokens
    cdef str tok
    cdef int idx
    cdef list keywords
    cdef str errorMsg
    cdef int minInt
    cdef int maxInt

    def __init__(self, s):
        self.tokens = []
        lexer = shlex.shlex(s)
        try:
            for token in lexer:
                self.tokens.append(str(token))
        except ValueError as e:
            raise RuntimeError(e)
        self.tokens.append('') # end token
        self.tok = self.tokens[self.idx]
        self.idx = 0
        self.keywords = ['create','table','insert','into','values',\
               'int','varchar','primary', 'key','select', 'from', 'where',\
                'and', 'or', 'as', 'count', 'sum', 'hashindex', 'treeindex','on']
        self.errorMsg = 'Wrong SQL syntax!'
        self.minInt = -2147483648
        self.maxInt = 2147483647


    
    cpdef bint matchDelim(self, delim) except *:
        return delim == self.tok

    cpdef bint matchNum(self) except *:
        if self.matchDelim('-'): # handling negative number
            if re.match('^\d+$', self.getNextTok()):
                return True
            else:
                return False
        if re.match('^\d+$', self.tok):
            return True
        return False

    cpdef bint matchVarchar(self) except *:
        if re.match('^\'.*\'$', self.tok):
            return True
        if re.match('^\".*\"$', self.tok):
            return True
        return False
    
    cpdef bint matchKeyword(self, keyword) except *:
        return keyword == self.tok.lower() and keyword in self.keywords
    
    cpdef bint matchId(self) except *:
        if re.match('^[a-z_]\w*', self.tok.lower()) and \
            self.tok.lower() not in self.keywords:
            return True
        return False
    
    cpdef str eatDelim(self, delim):
        if self.matchDelim(delim):
            self.nextToken()
            return delim
        else:
            print('delim error')

            raise RuntimeError('Invalid delimiter: \''+self.tok+'\'')

    cpdef int eatNum(self):
        cdef int num = 0
        if self.matchNum():
            if self.matchDelim('-'):
                self.eatDelim('-')
                num = -int(self.tok)
            else:
                num = int(self.tok)
            if num < self.minInt or num > self.maxInt:
                raise RuntimeError('Integer out of bound.')
            self.nextToken()
            return num
        else:
            print('num error')
            raise RuntimeError('Invalid number: ' + self.tok)

    cpdef str eatVarchar(self):
        cdef str chara = self.tok[1:-1]
        if self.matchVarchar():
            self.nextToken()
            return chara
        else:
            print('varchar error')
            raise RuntimeError('Invalid varchar: \'' + self.tok + '\'')

    cpdef str eatKeyword(self, keyword):
        if self.matchKeyword(keyword):
            self.nextToken()
            return keyword
        else:
            print('keyword error')
            raise RuntimeError('Unkown keyword: ' + self.tok)

    cpdef str eatId(self):
        cdef str chara = self.tok.lower()
        if self.matchId():
            self.nextToken()
            return chara
        else:
            print('id error')
            raise RuntimeError('Invalid identifier: ' + self.tok)
    
    cpdef nextToken(self):
        if self.idx + 1 < len(self.tokens):
            self.idx = self.idx + 1
            self.tok = self.tokens[self.idx]

    cpdef str getNextTok(self):
        cdef str tok = self.tok
        if self.idx + 1 < len(self.tokens):
            tok =  self.tokens[self.idx+1]
        return tok
