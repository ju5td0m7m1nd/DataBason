import os
import re
import time
from database import Database
from table_schema import Table

def runQuery(db):
    que = raw_input("fucking shit\n")
    # parse query list.
    query_list = re.split(';', que.strip())
    if '' in query_list:
        query_list.remove('')

    for query in query_list:
        start_time = time.time()
        table = db.processQuery(query)
        elapsed_time = time.time() - start_time
        print elapsed_time

    return table
    #with open('fucking_shit.txt', 'w') as f:
    #    f.write()


if __name__ == '__main__':
    db = Database()
    while(1):
        t = runQuery(db)
