#!/usr/bin/env python3
from src.core.errlog import errdata,mklog
import psycopg2 as psql
import time

# Main push-to-psql entry point
def export(data,project,config):
    db  = config['database']
    tbl = config['table']
    fields = data[0]._fields
    if 'custom-insertion' in config:
        ins = custom_insertion(fields,config['custom-insertion'])
    else: ins = ','.join(['%s'] * len(fields))
    cmd = 'INSERT INTO {} VALUES ({})'.format(tbl,ins)
    errs,errtxt = exec_push(data,cmd,db)
    if errs: errdata(project,data,txt='psqlerr')
    for err in errtxt: mklog(project,err)

# Actually push the stuff
def exec_push(data,cmd,db):
    errs,errtxt = [],[]
    dupcount = 0
    print('pushing {} rows to database: {}'.format(len(data),db))
    with psql.connect(database=db) as con:
        for row in data:
            try:
                with con.cursor() as cur:
                    cur.execute(cmd,row)
                con.commit()
            except Exception as err:
                if 'duplicate key' in str(err):
                    dupcount += 1
                    con.rollback()
                else:
                    errs.append(row)
                    errtxt.append(err)
                    con.rollback()
    con.close()
    if dupcount:
        print('{} duplicate rows ignored'.format(dupcount))
    return errs,errtxt

# Generate a custom insertion string.
def custom_insertion(fields,insmap):
    inserts = {
        'default' : '%s',
        'to-timestamp' : 'to_timestamp(%s)'
    }
    for i in insmap:
        if i not in fields:
            raise Exception('unrecognized field: {}'.format(i))
        if insmap[i] not in inserts:
            raise Exception('unrecognized type conversion: {}'.format(insmap[i]))
    ins = []
    for f in fields:
        if f in insmap:
            ins.append(inserts[insmap[f]])
        else: ins.append(inserts['default'])
    return ','.join(ins)
