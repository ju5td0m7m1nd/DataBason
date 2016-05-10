import os
import re
import time
import sys
from database import Database
from table_schema import Table

def runQuery(db):
    que = raw_input("$MAHSEEKO (type exit to leave)-> ")
    if que == 'exit':
        sys.exit('bye')
    # parse query list.
    query_list = re.split(';', que.strip())
    if '' in query_list:
        query_list.remove('')

    for query in query_list:
        t = time.time()
        table = db.processQuery(query)
        d = time.time() - t
        print (d)
    if table == []:
        pass 
    else: 
        with open('fucking_shit.txt', 'w') as f:
            k = list(table)
            for r in range(len(table[k[0]])):
                for i in range(len(k)):
                    f.write(str(table[k[i]][r]) + "\t\t")
                f.write("\n")
            f.close()
            
def gogo():
    db = Database()
    t = runQuery(db)
    return t       

if __name__ == '__main__':
    db = Database()
    while(1):
        t = runQuery(db)
