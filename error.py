class SqlSyntaxError(RuntimeError):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class SqlSemanticError(RuntimeError):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)
