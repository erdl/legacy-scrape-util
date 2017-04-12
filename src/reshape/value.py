#!/usr/bin/env python3
from src.core.error_utils import errdata
import src.core.data_utils as du

# Determines order of application
# during mulit-phase reshaping process.
ORD = 0

# reshape : proj -> conf -> state -> data -> (state,data)
def reshape(project,config,state,data):
    settings = config['settings']
    actions = ['filters','generators','mappings']
    if 'in-order' in settings:
        order = settings['in-order']
    else: order = [a for a in actions if a in config]
    errors = []
    for action in order:
        if action == 'filters':
            data = run_filters(project,config,data)
        elif action == 'generators':
            data = run_generators(project,config,data)
        elif action == 'mappings':
            data = run_mappings(project,config,data)
        else: raise Exception('unknown action: ' + action)
    return state,data

# Filter out undesired data-points, either
# by name of data-point, or by value range.
def run_filters(project,config,data):
    settings = config['settings']
    filters = config['filters']
    binary = [f for f in filters if isinstance(filters[f],bool)]
    ranged = [f for f in filters if isinstance(filters[f],dict)]
    uidsort = sort_by_uid(data)
    removed = []
    for uid in binary:
        if not binary[uid]: continue
        removed += uidsort.pop(uid,())
    for uid in ranged:
        if not uid in uidsort: continue
        rows = uidsort[uid]
        if 'max' in ranged[uid]:
            fltr = lambda r: r <= ranged[uid]['max']
            rows,remove = split(fltr,rows)
            removed += remove
        if 'min' in ranged[uid]:
            fltr = lambda r: r >= ranged[uid]['min']
            rows,remove = split(fltr,rows)
            removed += remove
        uidsort[uid] = rows
    filtered = []
    for rows in uidsort.values():
        filtered += rows
    handle_removals(project,config,'filter',removed)
    return filtered

# Generate one or more new sets of data-points
# by adding and subtracting `value` fields.
def run_generators(project,config,data):
    generators = config['generators']
    generators = check_generators(project,generators)
    uidsort = sort_by_uid(data)
    generated = []
    partials = {}
    rows = []
    for gen in generators:
        gid = uid_from_spec(gen)
        add,sub = [],[]
        addcount = len(gen['add'])
        subcount = len(gen['sub'])
        for uid in gen['add']:
            if uid in uidsort:
                add += uidsort[uid]
        for uid in gen['sub']:
            if uid in uidsort:
                sub += uidsort[uid]
        tadd = sort_by_timestamp(add)
        tsub = sort_by_timestamp(sub)
        completes = {}
        tindexes = [t for t in tadd] + [t for t in tsub if not t in tadd]
        for t in tindexes:
            base = {}
            base['add'] = tadd[t] if t in tadd else []
            base['sub'] = tsub[t] if t in tsub else []
            ac = len(base['add'])
            sc = len(base['sub'])
            if ac == addcount and sc == subcount:
                completes[t] = base
            else:
                if not gid in partials:
                    partials[gid] = {}
                partials[gid][t] = base
        for t in completes:


        for tindex in completes:


    return data,errors


# Generate one or more new sets of data-points
# by adding and subtracting `value` fields.
def run_generators(project,config,data):
    generators = config['generators']
    generators = check_generators(project,generators)
    uidsort = sort_by_uid(data)
    generated = []
    partials = {}
    rows = []
    for gen in generators:
        gid = uid_from_spec(gen)
        acount = len(gen['add'])
        scount = len(gen['sub'])
        sort = {} # { timestamp: {action: { uid: value } } }
        for row in data:
            uid = du.get_uid(row)
            if uid in gen['add']:
                action = 'add'
            elif uid in gen['sub']:
                action = 'sub'
            else: action = None
            if not action: continue
            tid = str(row.timestamp)
            if not tid in sort:
                sort[tid] = {'add': {}, 'sub': {}}
            sort[tid][action][uid] = row.value
        for tid in sort:
            ac = len(sort[tid]['add'])
            sc = len(sort[tid]['sub'])
            if not ac == acount or not sc == scount:
                if not gid in partials:
                    partials[gid] = {}
                partials[gid][tid] = sort[tid]
            ## CONTINUE


        for tindex in completes:


    return data,errors



def check_generators(project,generators):
    default = lambda: {'node':project,'unit':'undefined','sub':[]}
    required = ['name','add']
    checked = []
    for gen in generators:
        for req in required:
            if not req in gen:
                raise Exception('generator missing required field: '+req)
        new = default()
        new.update(gen)
        checked.append(new)
    return checked

# Map itentiy-level values to new identity-level
# values (eg; name="foo" to name="bar").
def run_mappings(project,config,data):
    mappings = config['mappings']

    errors = []

    return data,errors


def compress_rows(rows):
    compressed = {}
    for row in rows:
        uid = du.get_uid(row)


def sort_by_uid(rows):
    udisort = {}
    for row in rows:
        uid = du.get_uid(row)
        if not uid in uidsort:
            uidsort[uid] = []
        uidsort[uid].append(row)
    return uidsort


def sort_by_timestamp(rows):
    timesort = {}
    for row in rows:
        tid = str(int(row.timestamp))
        if not tid in timesort:
            timesort[tid] = []
        timesort[tid].append(row)
    return timesort

def split(fltr,rows):
    passes,fails = [],[]
    for row in rows:
        if fltr(row):
            passes.append(row)
        else: fails.append(row)
    return passes,fails

# generate a uid from a dictionary specifying,
# at the very least, the `name` field of some
# data-point.
def uid_from_spec(project,spec):
    if not 'name' in spec:
        raise Exception('`name` required for row spec: ' + str(spec))
    name = spec['name']
    node = spec['node'] if 'node' in spec else project
    unit = spec['unit'] if 'node' in unit else 'undefined'
    uid = '-'.join((node,name,unit)).lower()
    return uid


def handle_removals(project,config,step,rows):
    if not rows: return
    key = 'on-{}'.format(step)
    if not key in config['settings']: return
    action = config['settings'][key]
    if action == 'discard': return
    elif action == 'archive':
        du.save_archive(project,step,rows)
    else:
        raise Exception('unrecognized action: ' + action)











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
