import database
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
