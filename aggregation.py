import database

class Aggregation:
    
    '''
    get table from HandleSelect
    handle COUNT(), SUM()
    '''

    def __init__(self):
        self.counted = 0

    def count(self, returnTable, column_name, match_pair, table_name):
        '''
        load table from loadTable with something like this:
        then get the table from returnTable
        '''
        # count number of column_name: COUNT(column_name)
        if column_name == '*':
            if type(match_pair) is int :
                if table_name == '*':
                    tableLength = []
                    for table in returnTable:
                        tableLength.append(len(returnTable[table].records))
                    if len(tableLength) > 1:
                        self.counted = tableLength[0] * tableLength[1]
                    else:
                        self.counted = tableLength[0] 
                else:  
                    self.counted = len(returnTable[table_name].records)
            else:
                self.counted = len(match_pair)
        else:
            if type(match_pair) is int:
                self.counted = len(returnTable[table_name].records)
            else:
                for pair in match_pair:
                    key = pair[table_name]
                    value = returnTable[table_name].records[key][column_name]
                    if value != None:
                        self.counted += 1
        
        # handle return table (rename AS blablabla)
        returnCol = 'COUNT('+column_name+')'
        to_return = {returnCol: [self.counted]}
        return [self.counted]

    def sum(self, returnTable, column_name, match_pair, query_from):
        '''
        from returnTable, we need to re-assign returnTable in real practice.
        '''
        self.to_sum = 0
        # sum them up
        if type(match_pair) is int :
            for pk in returnTable[query_from].records:
                try:
                    self.to_sum += returnTable[query_from].records[pk][column_name]
                except TypeError:
                    continue
        else:
            '''
            for k in returnTable[query_from].records.keys():
                for m in match_pair:
                    if k == m[query_from]:
                        try:
                            self.to_sum += returnTable[query_from].records[k][column_name]
                        except TypeError:
                            continue
            '''
            for m in match_pair:
                key = m[query_from]
                try :
                    self.to_sum+=returnTable[query_from].records[key][column_name]
                except TypeError:
                    continue
        returnCol = 'SUM('+column_name+')'
        to_return = {returnCol: [self.to_sum]}
        return [self.to_sum]


