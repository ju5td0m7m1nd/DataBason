import database


'''
Handle query flow :

1. load table with the attribute in query['from'].
    *Error Raise : table not exist.
    *Error Raise : table alias duplicate.
2. Use the condition in query['where'], filter the columns.
    *Error Raise : check columns from non-loaded table.
    *Error Raise : check non-exist columns.
3. Check query['select'], determine which columns should be return (display).
    *Error Raise : select non-exist columns.
    *Error Raise : select non-loaded table.

'''

class HandleSelect:
    '''
    ISSUE : Should we pass the db reference in,
            in order to make share it located in same place
    '''
    returnTables = {} 
    def __init__(self,db):
        self.db = db   

    def loadTable(self,queryFrom):
        returnTables = {}
        db = self.db
        for unitquery in queryFrom :
            # Check table exist in db.tables.
            tableName = unitquery['tableName']
            if db.tables[tableName]:
                # Check if use alias 
                if not unitquery['alias'] == "":
                    # Check if alias duplicate
                    if unitquery['alias'] in returnTables:
                        raise RuntimeError("Alias name duplicate");
                    else:
                        returnTables[unitquery['alias']] = db.tables[tableName]
                else:
                    returnTables[tableName] = db.tables[tableName]
            else:
                raise RuntimeError ("Select from table which doesn't exist") 
        self.returnTables = returnTables 
    
    def checkWhere(self,queryWhere):
        logic = queryWhere['logic']
        condition = []
        condition.append(queryWhere['term1'])  
        condition.append(queryWhere['term2'])
       
        for c in condition:  
            if c :
                exp1 = c['exp1']
                exp2 = c['exp2']
                op = c['operator']
  
        return self.returnTables

    '''
    Exp may has these type :
    1. int
    2. string
    3. a.column
    4. column
    So we have to determine which type it is
    '''

    def determineExpression(self,exp):
        # exp is a string.
        if '\"' in exp: 
            return exp.split('\"')[1].split('\"')[0]
        # exp is number.
        elif type(exp) is int:
            return exp
        # exp with a prefix.
        elif '.' in exp :
            prefix = exp.split('.')[0]
            name = exp.split('.')[1]
            # Check prefix exist
            if prefix in self.returnTables:
                #Check column exist
                if  name in self.returnTables[prefix].attributeList :
                    expDict = {}
                    records = self.returnTables[prefix].records
                    for row in records:    
                        expDict[row] = records[row][name]
                    print records[row][name]
                    return expDict
                else:
                    raise RuntimeError ("Column %s not in Table %s",name,prefix)
            else :
                raise RuntimeError ("Table: " + prefix + " doesn't be loaded")
        else :
            # No prefix need to check exist in either table a or b.
            if len(self.returnTables) > 1:
                # A flag to record if there are more than 1 table have some column.
                flag = 0
                tableName = '' 
                for table in self.returnTables:
                    if exp in table:
                        flag = flag + 1 
                        tableName = table
                # if flag more than one, raise error
                if flag > 1 :
                    raise RuntimeError ("Column %s not distinct",exp)
                else :
                    expDict = {}
                    for row in self.returnTables[tableName]:
                        expDict[row] = self.returnTables[tableName][exp]
                return expDict                
  
            else :
                # check column in table
                for table in self.returnTables :
                    if exp in table :
                        # a dictionary contain only {row[pk] : row[exp]} .
                        expDict = {}
                        for row in table:
                            expDict[row] = table[exp]
                            return expDict
                    else :
                        raise RuntimeError ("Column not in table %s", exp)