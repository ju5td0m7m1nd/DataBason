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
    query = {}
    '''
    ISSUE : Should we pass the db reference in,
            in order to make share it located in same place
    '''
 
    def __init__(self,query,db):
        self.query = query  
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
         
        return returnTables 
