import database

class Aggregation:
    
    '''
    get table from HandleSelect
    handle COUNT(), SUM()
    '''

    def __init__(self):
        self.counted = 0

    def count(self, returnTable, column_name, match_pair):
        '''
        load table from loadTable with something like this:
        then get the table from returnTable
        '''
        # count number of column_name: COUNT(column_name)
        self.counted = len(match_pair)
        # handle return table (rename AS blablabla)
        returnCol = 'COUNT('+column_name+')'
        to_return = {returnCol: [self.counted]}
        return to_return

    def sum(self, returnTable, column_name, match_pair, query_from):
        '''
        from returnTable, we need to re-assign returnTable in real practice.
        '''
        self.to_sum = 0
        b_f = False
        # sum them up
        for k in returnTable[query_from].records.keys():
            for m in match_pair:
                if k == m[query_from]:
                    try:
                        self.to_sum += returnTable[query_from].records[k][column_name]
                    except TypeError:
                        self.to_sum = 0
                        b_f = True
                        print "Unsupported sum error near SUM()"
                        break
            if b_f:
                break

        returnCol = 'SUM('+column_name+')'
        to_return = {returnCol: [self.to_sum]}
        return to_return


