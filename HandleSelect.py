import database
import aggregation

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
    def __init__(self,db,query):
        self.db = db  
        self.query = query 

    def executeQuery(self):
        self.loadTable(self.query['from'])
        self.checkWhere(self.query['where'])
        self.checkSelect(self.query['select'])  
        return self.selectResult
    
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
                raise RuntimeError ("LoadTable : Select from table which doesn't exist") 
        self.returnTables = returnTables 
    
    def checkWhere(self,queryWhere):
        compareResult = [] 
        if len(queryWhere):  
            condition = []
            logic = queryWhere['logic']
            condition.append(queryWhere['term1'])  
            condition.append(queryWhere['term2'])
            for c in condition:  
                if c :
                    exp1 = self.determineExpression(c['exp1'])
                    exp2 = self.determineExpression(c['exp2'])
                    op = c['operator']
                    # Always make exp1 as list 
                    # Case 1, exp1 : int exp2 : list swap(exp1,exp2)
                    # Case 2, exp1 : str exp2 : list swap(exp1,exp2)    
                    if type(exp2) is list :                 
                        temp = exp2
                        exp2 = exp1
                        exp1 = temp                    
                    
                    # Due to previous swap, if exp1 not dict, and both of them is not dict
                    if not type(exp1) is list:
                        #Directly do logical operation
                        RESULT = False
                        if op == "=":
                            RESULT = (exp1 == exp2 )
                        elif op == ">":
                            RESULT = (exp1 > exp2 )
                        elif op == "<":
                            RESULT = (exp1 < exp2 )
                        elif op == "<>":
                            RESULT = (exp1 != exp2 )
                        pairList = []
                        if RESULT :
                            # Need an efficient way to join 2 table.
                            for t in self.returnTables:
                                # If pair list is not init.
                                if not len(pairList):
                                    for key in self.returnTables[t].records:
                                        pairList.append({t:key})
                                else :
                                    tempList = []  
                                    for key in self.returnTables[t].records:
                                        for l in pairList:
                                            for pairKey in l:
                                                tempList.append({pairKey:l[pairKey],t:key})
                                    pairList = tempList     
                        compareResult.append(pairList) 
                    else:
                        compareResult.append(self.filterRow(exp1,exp2,op))
                    self.compareResult = compareResult
        # Handle logical merge
        # Only when compareResult have more than one item
        # need to merge
        if len(compareResult) > 1 :
            self.matchPair = self.logicalMerge(compareResult,logic)
        elif len(compareResult) == 0:
            if len(self.returnTables) == 1:
                matchPair = []
                for t in self.returnTables:
                    for key in self.returnTables[t].records:
                        matchPair.append({t:key})
                self.matchPair = matchPair 
            elif len(self.returnTables) > 1 :
                for i in range(0,len(self.returnTables)-1):
                    matchPair = []
                    tableName1 = self.returnTables.keys()[i]  
                    tableName2 = self.returnTables.keys()[i+1]
                    for pk1 in self.returnTables[tableName1].records :
                        for pk2 in self.returnTables[tableName2].records:  
                            matchPair.append({tableName1:pk1,tableName2:pk2})
                self.matchPair = matchPair
        else :
            self.matchPair = self.logicalMerge(compareResult,None)
    
    def checkSelect(self,selectQuery): 
        requestList = selectQuery['fieldNames']
        agg = selectQuery['aggFn']
        selectResult = {}
        #store table which is already iteraed
        PRETABLE = []
        if len(agg):
            for aggPair in agg:
                if self.selectColumnValid([aggPair['field']]):
                    aggType = aggPair['type']
                    aggField = aggPair['field'] 
                    if '.' in aggField :
                        tableName = aggField.split('.')[0] 
                        columnName = aggField.split('.')[1]
                    else :
                        for t in self.returnTables:
                            if aggField in self.returnTables[t].attributeList:
                                tableName = t
                                columnName = aggField
                            elif aggField == '*':
                                tableName = '*'
                                columnName = '*' 
                    aggInstance = aggregation.Aggregation()
                    if aggType == 'count':
                        selectResult['COUNT('+aggField+')'] = aggInstance.count(self.returnTables,columnName,self.matchPair,tableName)
                    elif aggType == 'sum':
                        selectResult['SUM('+aggField+')'] = aggInstance.sum(self.returnTables,columnName,self.matchPair,tableName) 
                    else :
                        raise RuntimeError ("CheckSelect : Unknown aggregation type.")             
        else:
            if self.selectColumnValid(requestList):
                for request in requestList:
                    result = []
                    if '.' in request :
                        tableName = request.split('.')[0] 
                        columnName = request.split('.')[1]
                    else :
                        for t in self.returnTables:
                            if request in self.returnTables[t].attributeList:
                                tableName = t
                                columnName = request
                            elif request == '*':
                                tableName = '_ALLTABLE'
                                columnName = request
                    if columnName == '*':
                        for p in self.matchPair:
                            if tableName == '_ALLTABLE':
                                for t in p :
                                    pk = p[t] 
                                    for c in self.returnTables[t].records[pk]:
                                        if not c in selectResult:
                                            selectResult[c] = [self.returnTables[t].records[pk][c]]
                                        else:
                                            value = self.returnTables[t].records[pk][c]
                                            index = self.matchPair.index(p)
                                            listLength = len(selectResult[c])
                                            if index == listLength :
                                                selectResult[c].append(value)
                                            else:
                                                selectResult[c][index] = value                 
                            else:
                                pk = p[tableName]
                                for c in self.returnTables[tableName].records[pk]:
                                    OVERRIDE = False
                                    for pt in PRETABLE:
                                        if c in self.returnTables[pt].attributeList:
                                            OVERRIDE = True
                                            break
                                    if not c in selectResult:
                                        selectResult[c] = [self.returnTables[tableName].records[pk][c]]
                                    else:
                                        value = self.returnTables[tableName].records[pk][c]
                                        if OVERRIDE :
                                            selectResult[c][self.matchPair.index(p)] = value 
                                        else:
                                            selectResult[c].append(value)
                    else:
                        OVERRIDE = False
                        for pt in PRETABLE:
                            if columnName in selectResult.keys():
                                OVERRIDE = True
                                break
                        for p in self.matchPair:
                            pk = p[tableName]
                            value = self.returnTables[tableName].records[pk][columnName]
                            if OVERRIDE:
                                selectResult[columnName][self.matchPair.index(p)] = value
                            else: 
                                result.append(value)
                        if not OVERRIDE:
                            selectResult[columnName] = result    
                    PRETABLE.append(tableName)    
        self.selectResult = selectResult
    
    def selectColumnValid(self,requestList):
        for column in requestList:
            if '.' in column : 
                tableName = column.split('.')[0] 
                columnName = column.split('.')[1] 
                if tableName in self.returnTables :
                    if columnName == '*':
                        continue
                    elif columnName in self.returnTables[tableName].attributeList:
                        continue 
                    else :
                        raise RuntimeError ("SelectColumnValid : Unknown column name: "+columnName+ " in table: "+tableName)  
                else :
                    raise RuntimeError("SelectColumnValid : Unknown table name "+tableName )
            else :
                if column == '*':
                    continue
                else:
                    tableCount = 0
                    for table in self.returnTables :
                        if  column in self.returnTables[table].attributeList: 
                            tableCount = tableCount + 1 
                    if tableCount == 0 :
                        raise RuntimeError ("SelectColumnValid : Unknown column name.")
                    elif tableCount > 1 :
                        raise RuntimeError ("SelectColumnValid : Column name not distict.")
                    else:
                        continue
        return True   
    '''
    Logic : AND OR None
    compareResult : Dict, bool
    '''
    def logicalMerge(self,compareResult,logic):
        if logic == None:
            if type(compareResult[0]) is list:
                print compareResult[0]
                return compareResult[0]
            else:
                raise RuntimeError ('logicalMerge : Merge with unknown type')
        else :
            resultStandard = []
            if logic == 'and':
                for cr in compareResult:
                    if not len(cr):
                        return []
                    #if not len(resultStandard):
                    #    for pair in cr:
                    #        resultStandard.append(pair)
                    #else:
                    #    for r in resultStandard:
                    #        if not r in cr:
                    #            resultStandard.remove(r)
                for pair1 in compareResult[0]:
                    for pair2 in compareResult[1]:
                        if pair1 == pair2 :
                            resultStandard.append(pair1)
                return resultStandard
            elif logic == 'or':
                for cr in compareResult:
                    if not len(resultStandard):
                        for pair in cr:
                            resultStandard.append(pair)
                    else:
                        for pair in cr:
                            if not pair in resultStandard:
                                resultStandard.append(pair)                   
                return resultStandard
            else :
                raise RuntimeError ('logicalMerge : Unknown logic')
  
    '''
    Return a filtered dict
    '''
    def filterRow(self, exp1,exp2,op):
            #Create a new dict to store compare results.
            #Init
            pairList = []
            if type(exp2) is list:
                # use sorted list to accelerate equality join
                if op == '=':
                    exp1_sorted = sorted(exp1, key = lambda x : x['value']) 
                    exp2_sorted = sorted(exp2, key = lambda x : x['value'])
                    i = 0
                    j = 0
                    while i < len(exp1_sorted) and j < len(exp2_sorted):
                        record1 = exp1_sorted[i]
                        record2 = exp2_sorted[j]
                        if record1['value'] == record2['value'] :
                            pairList.append({record1['tableName'] : record1['pk'],record2['tableName']:record2['pk']})
                            i += 1
                            j += 1
                        elif record1['value'] > record2['value']:
                            j += 1
                        elif record1['value'] < record2['value']:
                            i += 1  
                else : 
                    for dict1 in exp1 :
                        value1 = dict1['value']
                        for dict2 in exp2:
                            flag = False
                            value2 = dict2['value']
                            if type(value1) != type(value2):
                                raise RuntimeError("different types and cannot be compared.")
                            if op == ">":
                                if value1 > value2:
                                    flag = True 
                            elif op == "<":
                                if value1 < value2:
                                    flag = True 
                            elif op == "<>" :
                                if value1 != value2:
                                    flag = True
                            else :
                                raise RuntimeError("FilterRow : Invalid operation " +op+" ")
                            if flag :
                                tableName_exp1 = dict1['tableName']
                                tableName_exp2 = dict2['tableName']
                                pairList.append({tableName_exp1:dict1['pk'],tableName_exp2:dict2['pk']})
            else:
                for dict1 in exp1 :
                    flag = False
                    if type(dict1['value']) != type(exp2):
                        raise RuntimeError('different types and cannot be compared.')
                    if op == "=":
                        if dict1['value'] == exp2:
                            flag = True
                    elif op == ">":
                        if dict1['value'] > exp2:
                            flag = True
                    elif op == "<":
                        if dict1['value'] < exp2:
                            flag = True
                    elif op == "<>":
                        if dict1['value'] != exp2:
                            flag = True
                    else :
                        raise RuntimeError("filterRow : Invalid operation "+op+" ")
                    if flag:
                        tableName = dict1['tableName']
                        #Need to join table.
                        if len(self.returnTables) > 1:
                            for t in self.returnTables:
                                if not t == tableName:
                                    for recordsKey in self.returnTables[t].records:   
                                        pairList.append({tableName:dict1['pk'],t:recordsKey})
                        else :
                            pairList.append({tableName:dict1['pk']})         
            return pairList 

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
                    expList = []
                    records = self.returnTables[prefix].records
                    for row in records:    
                        expList.append({'tableName':prefix,'value':records[row][name],'pk':row})
                    return expList
                else:
                    raise RuntimeError ("DetermineExpression : Column %s not in Table %s",name,prefix)
            else :
                raise RuntimeError ("DetermineExpression : Table: " + prefix + " doesn't be loaded")
        else :
            exp = exp.lower()
            expList = []
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
                    raise RuntimeError ("DetermineExpression : Column " + exp +" not distinct")
                elif flag == 0 :
                    raise RuntimeError ("DetermineExpression : No match column")
                else:
                    records = self.returnTables[tableName].records
                    for row in records:
                        expList.append({'tableName':tableName,'value':records[row][exp],'pk':row})
                    return expList 
  
            else :
                # check column in table
                for table in self.returnTables :
                    if exp in self.returnTables[table].attributeList :
                        # a dictionary contain only {row[pk] : row[exp]} .
                        records = self.returnTables[table].records
                        for row in records:
                            expList.append({'tableName':table,'value':records[row][exp],'pk':row})
                        return expList
                    else :
                        raise RuntimeError ("DetermineExpression : Column "+exp+ " not in table")
