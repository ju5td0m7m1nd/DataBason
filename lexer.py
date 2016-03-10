import re
import shlex

class Lexer:
    tokens = []
    idx = 0
    keywords = ['create','table','insert','into','values',\
               'int','varchar','primary', 'key']
    errorMsg = 'Wrong SQL syntax!'

    def __init__(self, s):
        self.tokens = []
        lexer = shlex.shlex(s)
        for token in lexer:
            self.tokens.append(str(token))
        self.tokens.append(None) # end token
        self.tok = self.tokens[self.idx]
    
    def matchDelim(self, delim):
        return delim == self.tok

    def matchNum(self):
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
        else:
            print 'delim error' # throw exception
            raise RuntimeError(self.errorMsg)

    def eatNum(self):
        if self.matchNum():
            num = int(self.tok)
            self.nextToken()
            return num
        else:
            print 'num error'
            raise RuntimeError(self.errorMsg)

    def eatVarchar(self):
        if self.matchVarchar():
            char = self.tok
            self.nextToken()
            return char
        else:
            print 'varchar error'
            raise RuntimeError(self.errorMsg)

    def eatKeyword(self, keyword):
        if self.matchKeyword(keyword):
            self.nextToken()
        else:
            print 'keyword error'
            raise RuntimeError(self.errorMsg)

    def eatId(self):
        if self.matchId():
            char = self.tok.lower()
            self.nextToken()
            return char
        else:
            print 'id error'
            raise RuntimeError(self.errorMsg)

    def nextToken(self):
        if self.idx + 1 < len(self.tokens):
            self.idx = self.idx + 1
            self.tok = self.tokens[self.idx]

