#!/usr/bin/env python3
from src.core.error_utils import error_template
from collections import namedtuple

# Defines order of
# execution in reshape runtime.
ORD = 2

field_error = error_template('`field` based data-reshaping step')

def reshape(project,config,state,data):
    data = map_fields(data,config)
    return state,data

# Map one or more fields to some other set of fields.
def map_fields(data,fieldmap):
    mkerr = field_error('field name & order reassignment')
    if not data: return []
    fieldmap = { k.lower() : fieldmap[k] for k in fieldmap }
    dfields  = data[0]._fields
    for field in fieldmap:
        if field not in dfields:
            error = mkerr('unrecognized field: ' + field)
            raise Exception(error)
    indexmap = []
    namemap  = []
    for di,df in enumerate(dfields):
        if not df in fieldmap: continue
        mf = fieldmap[df]['name']
        mi = fieldmap[df]['slot']
        indexmap.append((mi,di))
        namemap.append((mi,mf))
    sbi = lambda i: i[0]
    foi = lambda l: [x[1] for x in l]
    indexmap = foi(sorted(indexmap,key=sbi))
    namemap  = foi(sorted(namemap,key=sbi))
    fmtrow   = namedtuple('fmtrow', namemap)
    fmtdata  = []
    for d in data:
        row = []
        for i in indexmap:
            row.append(d[i])
        fmtdata.append(fmtrow(*row))
    return fmtdata
