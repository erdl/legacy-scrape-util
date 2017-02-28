#!/usr/bin/env python3
from src.core.errlog import errdata

ORD = 0

# reshape : proj -> conf -> data -> data
def reshape(project,config,data):
    data,err = map_content(data,config)
    if err: errdata(project,err)
    return data

# Map a set of values to some other set of values.
def map_content(data,valmap):
    valmap = { k.lower() : valmap[k] for k in valmap }
    dfields = data[0]._fields
    maperr = []
    mapped = []
    for field in valmap:
        if field not in dfields:
            raise Exception('invalid mapping: {}'.format(field))
        index = dfields.index(field)
        if 'ignore' in valmap[field]:
            ignore = [i.lower() for i in valmap[field]['ignore']]
        else: ignore = []
        fmap = valmap[field]['map']
        fmap = { k.lower() : fmap[k] for k in fmap }
        for d in data:
            r = [f for f in d]
            val = r[index]
            if val in fmap:
                r[index] = fmap[val]
                mapped.append(Row(*r))
            else:
                if val in ignore: continue
                else: maperr.append(Row(*r))
        data = [ r for r in mapped ]
        mapped = []
    return data,maperr
