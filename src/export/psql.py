#!/usr/bin/env python3
from src.core.errlog import errdata,mklog
import psycopg2 as psql
import time


# Main push-to-psql entry point
def export(data,project,config):
    db  = config['general']['database']
    tbl = config['general']['table']
    fields = data[0]._fields
    # handle custom-inserion instance if needed.
    if 'conversions' in config:
        ins = custom_insertion(fields,config['conversions'])
    # default is just standard psycopg2 formatting...
    else: ins = ','.join(['%s'] * len(fields))
    cmd = 'INSERT INTO {} VALUES ({})'.format(tbl,ins)
    errs,errtxt,duplicates = handle_push(data,cmd,db)
    if duplicates:
        print('duplicate rows ignored: ',duplicates)
    # save any rows which raised unexpexted errors
    # to a csv with prefix `psqlerr`.
    if errs: errdata(project,data,txt='psqlerr')
    # append any unexpected errors to
    # the project's main error log.
    for err in errtxt: mklog(project,err)

# Controller for the actual push attempt.
# Forces all rows to be attempted at least once.
# This is a workaround for the fact that psql,
# until recently, will invalidate entire transaction
# blocks if too many duplacates are encountered.
def handle_push(data,cmd,db):
    duplicates = 0
    errs,errtxt = [],[]
    while len(data) > 0:
        data,dup,err,txt = push_rows(data,cmd,db)
        if dup: duplicates += 1
        if err: errs.append(err)
        if txt: errtxt.append(txt)
    return errs,errtxt,duplicates

# Attempts to push rows to db.  Removes rows
# from data as they are pushed.  Halts & returns
# remaining rows upon first exception.
def push_rows(data,cmd,db):
    duplicate = False
    error = None
    text = None
    with psql.connect(database=db) as con:
        con.set_session(autocommit=True)
        for i in range(len(data)):
            row = data.pop(0)
            try:
                with con.cursor() as cur:
                    cur.execute(cmd,row)
            except Exception as err:
                if 'duplicate key' in str(err):
                    duplicate = True
                else:
                    error = row
                    text = str(err)
                break
    con.close()
    return data,duplicate,error,text

'''
# Actually push the stuff
def exec_push(data,cmd,db):
    errs,errtxt = [],[]
    dupcount = 0
    print('pushing {} rows to database: {}'.format(len(data),db))
    with psql.connect(database=db) as con:
        # activate autocommit so duplicate rows
        # don't kill the entire uplaod proecess.
        con.set_session(autocommit=True)
        for row in data:
            try:
                # cursor context manager handles cursor
                # related cleanup on exception; kinda slow to
                # use an individual cursor for each row, but
                # necessary when duplicate data is an issue.
                with con.cursor() as cur:
                    cur.execute(cmd,row)
            except Exception as err:
                # duplicate key errors are ignored to facilitate recovery
                # from partial uplaod, loss of nonce file, etc...
                if 'duplicate key' in str(err):
                    dupcount += 1
                else:
                    errs.append(row)
                    errtxt.append(err)
    con.close()
    if dupcount:
        print('{} duplicate rows ignored'.format(dupcount))
    return errs,errtxt
'''


# Generate a custom insertion string.
def custom_insertion(fields,insmap):
    # collection of all possible insertion strings.
    inserts = {
        'default' : '%s',
        'to-timestamp' : 'to_timestamp(%s)'
    }
    for i in insmap:
        if i not in fields:
            raise Exception('unrecognized data field: {}'.format(i))
        if insmap[i] not in inserts:
            raise Exception('unrecognized insertion type: {}'.format(insmap[i]))
    ins = []
    # build the custom insertion string field by field.
    for f in fields:
        if f in insmap:
            ins.append(inserts[insmap[f]])
        else: ins.append(inserts['default'])
    return ','.join(ins)
