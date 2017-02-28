#!/usr/bin/env python3
from collections import namedtuple

ORD = 4

# reshape : proj -> conf -> data -> data
def reshape(project,config,data):
    data = gen_fields(data,config)
    return data

# Add one or more generated fields.
def gen_fields(data,gfields):
    generators = { "upload-time" : upload_time() }
    errgf = [f for f in gfields if not f in generators]
    if errgf:
        raise Exception('invalid generated-field(s): {}'.format(errgf))
    dfields = list(data[0]._fields)
    gindex = []
    # populate gindex w/ (index,name,generator)
    for g in gfields:
        gi = []
        gi.append(gfields[g]['index'])
        gi.append(gfields[g]['name'])
        gi.append(g)
        gindex.append(tuple(gi))
    sbi = lambda i: i[0]
    foi = lambda l: [ x[1] for x in l ]
    fog = lambda l: [ x[2] for x in l ]
    gindex    = sorted(gindex,key=lambda i: i[0])
    nameindex = foi(gindex)
    genindex  = fog(gindex)
    row_new   = namedtuple('row',dfields + nameindex)
    data_new  = []
    for d in data:
        gf = [ generators[g]() for g in genindex ]
        r  = list(d) + gf
        data_new.append(row_new(*r))
    return data_new


# Returns generator for the
# "uplad-time" generated field.
def upload_time():
    t = time.time()
    return lambda : t
