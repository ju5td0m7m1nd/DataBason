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
                exp1 = self.determineExpression(condition['exp1'])
                exp2 = self.determineExpression(condition['exp2'])
                op = condition['operator']
                
                # Always make exp1 as dict
                # Case 1, exp1 : int exp2 : dict swap(exp1,exp2)
                # Case 2, exp1 : str exp2 : dict swap(exp1,exp2)    
                if type(exp2) is dict :                 
                    temp = exp2
                    exp2 = exp1
                    exp1 = temp                    
                
                # Due to previous swap, if exp1 not dict, and both of them is not dict
                if not type(exp1) is dict:
                    #Directly do logical operation
                    pass 
                else:
                    filterRow(exp1,exp2,op) 
 
        return self.returnTables
    '''
    Return a filtered dict
    '''

    def filterRow(self, exp1,exp2,op):
            #Create a new dict to store compare results.
            #Init
            newTable = {}
            for table in self.returnTables:
                newTable[table] = {}
            if type(exp2) is dict:
                #Iterate dict to compare.
                for key1 in exp1 :
                    value1 = exp1[key1]
                    for key2 in exp2:
                        flag = False
                        value2 = exp2[key2]
                        if op == "=":
                            if value1['value'] == value2['value']:
                                flag = True 
                        elif op == ">":
                            if value1['value'] > value2['value']:
                                flag = True 
                        
                        elif op == "<":
                            if value1['value'] < value2['value']:
                                flag = True 
                        else :
                            raise RuntimeError("Invalid operation " +op+" ")
                        if flag :
                            tableName_exp1 = value1['tableName']
                            tableName_exp2 = value2['tableName']
                            newTable[tableName_exp1][key1] = self.returnTables[tableName_exp1].records[key1]
                            newTable[tableName_exp2][key2] = self.returnTables[tableName_exp2].records[key2]

            else:
                for key in exp1 :
                    value = exp1[key]
                    flag = False
                    if op == "=":
                        if exp1[key]['value'] == exp2:
                            flag = True
                    elif op == ">":
                        if exp1[key]['value'] > exp2:
                            flag = True
                    elif op == "<":
                        if exp1[key]['value'] < exp2:
                            flag = True
                    else :
                        raise RuntimeError("Invalid operation "+op+" ")
                    if flag:
                        tableName = exp1[key]['tableName']
                        newTable[tableName][key] = self.returnTables[tableName].records[key]
            return newTable


    '''
    Exp may has these type :
    1. int
    2. string
    3. a.column
    4. column
    So we have to determine which type it is
    
    #ISSUE all of column is lowercase??

    '''

    def determineExpression(self,exp):
        
        # exp is number.
        if type(exp) is int:
            return exp
        # exp is a string.
        elif '\"' in exp: 
            return exp.split('\"')[1].split('\"')[0]
                # exp with a prefix.
        elif '.' in exp :
            exp = exp.lower()
            prefix = exp.split('.')[0]
            name = exp.split('.')[1]
            # Check prefix exist
            if prefix in self.returnTables:
                #Check column exist
                if  name in self.returnTables[prefix].attributeList :
                    expDict = {}
                    records = self.returnTables[prefix].records
                    for row in records:    
                        expDict[row] = {'tableName':prefix,'value':records[row][name]}
                    return expDict
                else:
                    raise RuntimeError ("Column %s not in Table %s",name,prefix)
            else :
                raise RuntimeError ("Table: " + prefix + " doesn't be loaded")
        else :
            exp = exp.lower()
            # No prefix need to check exist in either table a or b.
            if len(self.returnTables) > 1:
                # A flag to record if there are more than 1 table have some column.
                flag = 0
                tableName = '' 
                for table in self.returnTables:
                    tableSchema = self.returnTables[table]
                    if exp in tableSchema.attributeList:
                        flag = flag + 1 
                        tableName = table
                # if flag more than one, raise error
                if flag > 1 :
                    raise RuntimeError ("Column " + exp +" not distinct")
                elif flag == 0 :
                    raise RuntimeError ("No match column")
                else:
                    expDict = {}
                    records = self.returnTables[tableName].records
                    for row in records:
                        expDict[row] = {'tableName':tableName,'value':records[row][exp]}
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
