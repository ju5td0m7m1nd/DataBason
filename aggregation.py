import database
from HandleSelect import *

class Aggregation:
    
    '''
    get table from HandleSelect
    handle COUNT(), SUM()
    '''

    def __init__(self, db):
        self.db = db
        self.hs = HandleSelect(self.db)
        self.counted = 0

    def count(column_name):
        '''
        load table from loadTable with something like this:
            query = {'from': [{'alias': '', 'tableName': 'teachers'}}
        the above is:
            hs.loadTable(query['from']) or
            hs.loadTable([{'alias':'','tableName':'teachers'},{'alias':'','tableName':'students'}])

        then get the table from returnTable
        '''
        query = {'from': [{'alias': '', 'tableName': 'teachers'}]}
        self.hs.loadTable(query['from'])
        returnTable = self.hs.returnTable()

        # count number of column_name: COUNT(column_name)
        self.counted = len(returnTable.keys())
        # handle return table (rename AS blablabla)
        returnCol = 'COUNT('+column_name+')'
        to_return = {self.counted: {returnCol: self.counted}}
        return to_return

    def sum(column_name):
        '''
        from returnTable, we need to re-assign returnTable in real practice.
        '''
        query = {'from': [{'alias': '', 'tableName': 'teachers'}]}
        self.hs.loadTable(query['from'])
        returnTable = self.hs.returnTable()

        self.to_sum = 0
        # sum them up
        for k in returnTable.keys():
            try:
                self.to_sum += returTable[k][column_name]
            except TypeError:
                self.to_sum = 0
                print "Unsupported sum error near SUM()"
                break

        returnCol = 'SUM('+column_name+')'
        to_return = {self.to_sum: {returnCol: self.to_sum}}
        return to_return


