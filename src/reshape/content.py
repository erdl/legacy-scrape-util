#!/usr/bin/env python3
from src.core.errlog import errdata
from src.core.row import Row

# Determines order of application
# during mulit-phase reshaping process.
ORD = 0

# reshape : proj -> conf -> data -> data
def reshape(project,config,data):
    data,err = exec_mapping(data,config)
    if err: errdata(project,err)
    return data

# Iteratively launch the appropriate
# mapping function for each specified field.
def exec_mapping(data,config):
    errors = []
    ignores = 0
    for field in config:
        if not data: break
        if 'sub-map' in config[field]:
            vals,errs,ign = sub_map(data,field,config[field])
        else:
            vals,errs,ign = map_field(data,field,config[field])
        errors += errs
        ignores += ign
        data = vals
    print('rows ignored during remapping: {}'.format(ignores))
    return data,errors

# Recursively generate mapping with
# multiple field dependencies.
def sub_map(data,field,fmap):
    if not data: return [],[]
    fmt = lambda v: str(v).lower()
    mapped,maperr = [],[]
    subfield = fmt(fmap['sub-map'])
    index = data[0]._fields.index(subfield)
    if 'ignore' in fmap:
        ignore = [fmt(i) for i in fmap['ignore']]
    else: ignore = []
    ignores = 0
    ignrows = [ r for r in data if fmt(r[index]) in ignore ]
    vmap = fmap['map']
    vmap = { fmt(k) : vmap[k] for k in vmap }
    for val in vmap:
        rows = [ r for r in data if fmt(r[index]) == val ]
        if not rows: continue
        if 'sub-map' in vmap[val]:
            mrows,erows,ign= sub_map(rows,field,vmap[val])
        else:
            mrows,erows,ign= map_field(rows,field,vmap[val])
        mapped += mrows
        maperr += erows
        ignores += ign
        data = [ r for r in data if not r in rows ]
    maperr += data
    return mapped,maperr,ignores

# Map new values to a given field.
def map_field(data,field,fmap):
    mapped,maperr = [],[]
    index = data[0]._fields.index(field)
    if 'ignore' in fmap:
        ignore = [i.lower() for i in fmap['ignore']]
    else: ignore = []
    ignores = 0
    vmap = fmap['map']
    vmap = { k.lower() : vmap[k] for k in vmap }
    for r in data:
        l = list(r)
        v = l[index]
        if v in vmap:
            l[index] = vmap[v]
            mapped.append(Row(*l))
        elif v in ignore:
            ignores += 1
            continue
        else: maperr.append(Row(*l))
    return mapped,maperr,ignores
