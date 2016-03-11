from schema import Schema

class CreateTableData:

    def __init__(self, tableName, schema, primaryKey):
        self.tableName = tableName
        self.schema = schema
        self.primaryKey = primaryKey

