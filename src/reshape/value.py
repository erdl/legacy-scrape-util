#!/usr/bin/env python3
from src.core.errlog import errdata
from src.core.utils import Row

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
    rowcount = len(data)
    errors = []
    ignores = 0
    for field in config:
        if not data: break
        mappings,settings = split_config(config[field])
        if 'sub-map' in settings:
            vals,errs,ign = sub_map(data,field,mappings,settings)
        elif 'concat-map' in settings:
            vals,errs,ign = concat_map(data,field,mappings,settings)
        else:
            vals,errs,ign = map_field(data,field,mappings,settings)
        errors += errs
        ignores += ign
        data = vals
    assert rowcount == sum((ignores,len(errors),len(data)))
    print('rows ignored during remapping: {}'.format(ignores))
    return data,errors

def concat_map(data,field,mappings,settings):
    fields = list(Row._fields)
    concats = settings['concat-map']
    if 'ignores' in settings:
        ignores = settings['ignores']
    else: ignores = []
    findex = fields.index(field)
    cindexes = []
    for cfield in concats:
        if not cfield in fields:
            raise Exception('unrecognized field: ',cfield)
        cindexes.append(fields.index(cfield))
    fmt = lambda r: '-'.join((r[n] for n in cindexes)).lower()
    mapped,maperr,igncount = [],[],0
    for row in data:
        key = fmt(row)
        if key in mappings:
            values = list(row)
            values[findex] = mappings[key]
            mapped.append(Row(*values))
        elif key in ignores:
            igncount += 1
        else:
            maperr.append(row)
    return mapped,maperr,igncount

# Recursively generate mapping with
# multiple field dependencies.
def sub_map(data,field,mappings,settings):
    # return empty lists if no data left to work with.
    if not data: return [],[],[]
    # case sensitivity is annoying.
    fmt = lambda v: str(v).lower()
    # get the index of the field we are working with.
    index = data[0]._fields.index(settings['sub-map'])
    # let ignore default to an empty list.
    if not 'ignores' in settings: settings['ignores'] = []
    # assemble list of formatted ignore values.
    ignvals = list(map(fmt,settings['ignores']))
    # assemble list of rows which should be ignored.
    ignrows = [r for r in data if fmt(r[index]) in ignvals]
    # reduce data to its elements which are not being ignored.
    data = [r for r in data if not r in ignrows]
    # initialize our various collectors.
    mapped,maperr,igncount = [],[],len(ignrows)
    # iteratively execute all mappings.
    for val in mappings:
        # get all rows which contain the target value.
        rows = [ r for r in data if fmt(r[index]) == val ]
        # reduce data to its elements which did not contain target.
        data = [ r for r in data if not r in rows ]
        if not rows: continue
        # separate out the mappings and configurations for our target.
        valmap,valset = split_config(mappings[val])
        # if valset contains a sub-map, the recursion continues!
        if 'sub-map' in valset:
            mrows,erows,ign= sub_map(rows,field,valmap,valset)
        # otherwise, we have reached our final recursion.
        else: mrows,erows,ign= map_field(rows,field,valmap,valset)
        mapped += mrows # successfully mapped rows.
        maperr += erows # unsuccessfuly mapped rows.
        igncount += ign # tally of ignored rows.
    # any remaining rows contained values which were not ignored
    # and which also did not match any of the supplied mappings.
    maperr += data
    return mapped,maperr,igncount



# Map new values to a given field, returning lists
# of successfully and unsuccessfully mapped rows,
# as well as a tally of the number of rows ignored.
def map_field(data,field,mappings,settings):
    fmt = lambda v: str(v).lower()
    index = data[0]._fields.index(field)
    if 'ignores' in settings:
        ignore = [fmt(i) for i in settings['ignores']]
    else: ignore = []
    ignores = 0
    mapped,maperr = [],[]
    for r in data:
        l = list(r)
        v = fmt(l[index])
        if v in mappings:
            l[index] = mappings[v]
            mapped.append(Row(*l))
        elif v in ignore:
            ignores += 1
        else: maperr.append(Row(*l))
    return mapped,maperr,ignores

# split a given value-mapping configuration
# inot separate mappings and settings.
def split_config(config):
    fmt = lambda d: {str(k).lower():d[k] for k in d}
    if 'map' in config and 'set' in config:
        return fmt(config['map']),config['set']
    settings = {}
    setfields = ['ignores','sub-map','concat-map']
    for field in setfields:
        matches = sorted((k for k in config if k.endswith(field)))
        if not matches: continue
        for match in matches:
            if field == match.replace('_',''):
                settings[field] = config[match]
                del config[match]
                break
    mappings = fmt(config)
    return mappings,settings
