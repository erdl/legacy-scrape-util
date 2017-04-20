#!/usr/bin/env python3
from src.core.error_utils import errdata,mklog
import psycopg2 as psql
import time


# Main push-to-psql entry point
def export(project,config,state,data):
    settings = config['settings']
    db  = settings['database']
    tbl = settings['table']
    fields = data[0]._fields
    # handle custom-inserion instance if needed.
    if 'conversions' in config:
        ins = custom_insertion(fields,config['conversions'])
    # default is just standard psycopg2 formatting...
    else: ins = ','.join(['%s'] * len(fields))
    cmd = 'INSERT INTO {} VALUES ({})'.format(tbl,ins)
    print('pushing {} rows to psql...'.format(len(data)))
    errors,errtxt,duplicates = handle_push(data,cmd,db)
    if duplicates:
        print('duplicate rows ignored: ',len(duplicates))
    # save any rows which raised unexpexted errors
    # to a csv with prefix `psqlerr`.
    if errors: errdata(project,errors,txt='psqlerr')
    # if `save-duplicates` is flagged `true`, save
    # all duplicate rows to the errors directory.
    if duplicates and 'save-duplicates' settings:
        if settings['save-duplicates']:
            errdata(project,duplicates,txt='psqldups')
    # append any unexpected errors to
    # the project's main error log.
    for err in errtxt: mklog(project,err)
    return state

# Controller for the actual push attempt.
# Forces all rows to be attempted at least once.
# This is a workaround for the fact that psql,
# until recently, will invalidate entire transaction
# blocks if too many duplacates are encountered.
def handle_push(data,cmd,db):
    rowtotal = len(data)
    # make a copy of data to avoid
    # side-effects from pops/dels.
    rows = [r for r in data]
    uploaded,ignored = [],[]
    errors,errtxt = [],[]
    while len(rows) > 0:
        rows,upld,igns,err,txt = push_rows(rows,cmd,db)
        uploaded += upld
        ignored += igns
        if err: errors.append(err)
        if txt: errtxt.append(txt)
    assert rowtotal == sum((len(errors),len(ignored),len(uploaded)))
    return errors,errtxt,ignored

# Attempts to push rows to db.  Removes rows
# from data as they are pushed.  Halts & returns
# remaining rows upon first exception.
def push_rows(data,cmd,db):
    ignored,uploaded = [],[]
    error,text = None,None
    with psql.connect(database=db) as con:
        con.set_session(autocommit=True)
        for i in range(len(data)):
            row = data.pop(0)
            try:
                with con.cursor() as cur:
                    cur.execute(cmd,row)
                uploaded.append(row)
            except Exception as err:
                if 'duplicate key' in str(err):
                    ignored.append(row)
                else:
                    error = row
                    text = str(err)
                break
    con.close()
    return data,uploaded,ignored,error,text


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
