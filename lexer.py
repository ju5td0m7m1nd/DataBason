import re
import shlex

class Lexer:
    tokens = []
    idx = 0
    keywords = ['create','table','insert','into','values',\
               'int','varchar','primary', 'key']

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
        return keyword == self.tok and keyword in self.keywords
    
    def matchId(self):
        if re.match('^[A-Za-z_]\w+', self.tok) and \
            self.tok not in self.keywords:
            return True
        return False
    
    def eatDelim(self, delim):
        if self.matchDelim(delim):
            self.nextToken()
        else:
            print 'error1' # throw exception

    def eatNum(self):
        if self.matchNum():
            num = int(self.tok)
            self.nextToken()
            return num
        else:
            print 'error2'

    def eatVarchar(self):
        if self.matchVarchar():
            char = self.tok
            self.nextToken()
            return char
        else:
            print 'error3'

    def eatKeyword(self, keyword):
        if self.matchKeyword(keyword):
            self.nextToken()
        else:
            print 'error4'

    def eatId(self):
        if self.matchId():
            char = self.tok
            self.nextToken()
            return char
        else:
            print 'error5'

    def nextToken(self):
        if self.idx + 1 < len(self.tokens):
            self.idx = self.idx + 1
            self.tok = self.tokens[self.idx]

