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
        for table in queryFrom :
            # Check table exist in db.tables.
            tableName = table['tableName']
            if db.tables[tableName]:
                returnTables[tableName] = db.tables[tableName]
            else:
                raise RuntimeError ("Select from table which doesn't exist") 
            
        return returnTables 
