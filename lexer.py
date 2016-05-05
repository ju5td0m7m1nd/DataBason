import re
import shlex

class Lexer:
    tokens = []
    idx = 0
    keywords = ['create','table','insert','into','values',\
               'int','varchar','primary', 'key','select', 'from', 'where',\
                'and', 'or', 'as', 'count', 'sum', 'hashindex', 'treeindex','on']
    errorMsg = 'Wrong SQL syntax!'
    minInt = -2147483648
    maxInt = 2147483647

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
    
    def matchDelim(self, delim):
        return delim == self.tok

    def matchNum(self):
        if self.matchDelim('-'): # handling negative number
            if re.match('^\d+$', self.getNextTok()):
                return True
            else:
                return False
        if re.match('^\d+$', self.tok):
            return True
        return False

    def matchVarchar(self):
        if re.match('^\'.*\'$', self.tok):
            return True
        if re.match('^\".*\"$', self.tok):
            return True
        return False
    
    def matchKeyword(self, keyword):
        return keyword == self.tok.lower() and keyword in self.keywords
    
    def matchId(self):
        if re.match('^[a-z_]\w*', self.tok.lower()) and \
            self.tok.lower() not in self.keywords:
            return True
        return False
    
    def eatDelim(self, delim):
        if self.matchDelim(delim):
            self.nextToken()
            return delim
        else:
            print 'delim error'

            raise RuntimeError('Invalid delimiter: \''+self.tok+'\'')

    def eatNum(self):
        if self.matchNum():
            num = 0
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
            print 'num error'
            raise RuntimeError('Invalid number: ' + self.tok)

    def eatVarchar(self):
        if self.matchVarchar():
            char = self.tok[1:-1]
            self.nextToken()
            return char
        else:
            print 'varchar error'
            raise RuntimeError('Invalid varchar: \'' + self.tok + '\'')

    def eatKeyword(self, keyword):
        if self.matchKeyword(keyword):
            self.nextToken()
            return keyword
        else:
            print 'keyword error'
            raise RuntimeError('Unkown keyword: ' + self.tok)

    def eatId(self):
        if self.matchId():
            char = self.tok.lower()
            self.nextToken()
            return char
        else:
            print 'id error'
            raise RuntimeError('Invalid identifier: ' + self.tok)
    
    def nextToken(self):
        if self.idx + 1 < len(self.tokens):
            self.idx = self.idx + 1
            self.tok = self.tokens[self.idx]

    def getNextTok(self):
        tok = self.tok
        if self.idx + 1 < len(self.tokens):
            tok =  self.tokens[self.idx+1]
        return tok
