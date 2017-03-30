#!/usr/bin/env python3
from src.core.row import Row

# The `extra` feature of the data-acquisition
# step is designed for use in generating additional
# data-points based upon the data-points acquired
# during primary data-acquisition.  This is distinct
# from the `extra` feature of the data-reshaping step
# which is designed to allow for the generation
# of additional fields.  This helps us maintain
# interoperability between steps, by keeping clear
# distinctions between the responsibilities and expectatios
# that a given step has toward the broader workflow.

EXTENSIONS = {
    'calculated-row' : lambda c,s,d: generate_rows(c,s,d)
}

# primary entry point for `extra`.
def extend(config,state,data):
    # Expects config to be a list of independent
    # configurations.  Each configuration must at
    # a minimum implement a `type` field, corresponding
    # to one of the possible generators in `EXTENSIONS`.
    for exconf in config:
        if not exconf['type'] in EXTENSIONS:
            raise Exception('unrecognized data-extension type: {}'.format(exconf['type']))
        generator = EXTENSIONS[exconf['type']]
        # All generators must execpt the args (config,state,data),
        # and return a new instance of `state`, as well as any
        # additional rows which were generated.
        state,rows = generator(exconf,state,data)
        print('{} data points generated of type: {}'.format(len(rows),exconf['type']))
        # Newly generated rows are added immediately to data.
        # This allows for later generations to integrate the
        # outputs of earlier generations.
        data += rows
    # Returns new instances of data & state, with all
    # all modifications included.
    return data,state

# Generates one or more calculated rows, by matching
# on a series of field arguments, splitting all matches
# by timestamp, and adding/subtracting the content
# of the `value` fields.
def generate_rows(config,state,data):
    # Since the mappings for this feature can be quite complex,
    # we support the ability of the user to shunt the mappings
    # into a `config-file` field, which is expanded to `config`
    # when the main configuration file is loaded.
    if 'config' in config: config = config['config']
    dpc = config['data-point']
    # The newly generated row must have its `node` and `sensor`
    # values defined, but it can optionally omit units.
    if not 'unit' in dpc: dpc['unit'] = 'undefined'
    mkrow = lambda t,v: Row(dpc['node'],dpc['sensor'],dpc['unit'],t,v)
    # The new data point must specify one or more targets which
    # are added to produce its value, and may optionally
    # define one or more targets to be subtracted as well.
    addlist = assemble_rows(config['add'],data)
    if 'sub' in config:
        sublist = assemble_rows(config['sub'],data)
    else: sublist = []
    # All adds and subs are sorted by time index, with
    # any apparently incomplete time indexes added to
    # the state variable under the assumption that their
    # remaining values will appear next run.
    timesort,state = sort_time_points(config,state,addlist,sublist)
    rows = []
    roundto = 2
    if 'modify' in config:
        if 'round' in config['modify']:
            roundto = config['modify']['round']
    # iteratively generate the data-point for each time index.
    for tindex in timesort:
        value = sum(timesort[tindex]['add']) - sum(timesort[tindex]['sub'])
        rows.append(mkrow(float(tindex),round(value,roundto)))
    return state,rows

# Sorts the data-points to be added and subtracted
# by their time indexes.  Incomplete indexes have
# their values saved to the `state` variable.
def sort_time_points(config,state,addlist,sublist):
    partials,state = extract_partials(config,state)
    timesort = {}
    for r in addlist:
        tindex = str(r.timestamp)
        if not tindex in timesort:
            timesort[tindex] = {'add': [], 'sub': []}
        timesort[tindex]['add'].append(r.value)
    for r in sublist:
        tindex = str(r.timestamp)
        timesort[tindex]['sub'].append(r.value)
    for t in timesort:
        if t in partials:
            timesort[t]['add'] += partials[t]['add']
            timesort[t]['sub'] += partials[t]['sub']
    if 'expect' in config:
        expect = config['expect']
    else: expect = {}
    if not 'add-count' in expect:
        maxkey = max(timesort,key=lambda k: len(timesort[k]['add']))
        expect['add-count'] = len(timesort[maxkey]['add'])
    if not 'sub-count' in expect:
        maxkey = max(timesort,key=lambda k: len(timesort[k]['sub']))
        expect['sub-count'] = len(timesort[maxkey]['sub'])
    newpartials = {}
    check = lambda t,k: len(timesort[t][k]) == expect['{}-count'.format(k)]
    for t in timesort:
        if not check(t,'add') or not check(t,'sub'):
            newpartials[t] = timesort[t]
        else: continue
    for t in newpartials:
        del timesort[t]
    if newpartials:
        state = merge_partials(config,state,newpartials)
    return timesort,state

# Removes any partial time indexes corresponding to
# the current data-point generation from the `state`
# variable.  For conciceness, this is achieved by hashing
# the sensor and node values for the data-point.
def extract_partials(config,state):
    if not 'calculated-row' in state: return {},state
    if not 'partials' in state['calculated-row']:
        return {},state
    node = config['data-point']['node']
    sensor = config['data-point']['sensor']
    hashkey = str(abs(hash(node + sensor)))
    partials = state['calculated-row']['partials']
    if hashkey in partials:
        extract = partials[hashkey]
        del state['calculated-row']['partials'][hashkey]
    else: extract = {}
    if not state['calculated-row']:
        del state['calculated-row']
    return extract,state

# Merge any outstanding partial time indexes into
# the state variable for completion on the next
# iteration of the program. 
def merge_partials(config,state,partials):
    node = config['data-point']['node']
    sensor = config['data-point']['sensor']
    hashkey = str(abs(hash(node + sensor)))
    if not 'calculated-row' in state:
        state['calculated-row'] = {}
    if not 'partials' in state['calculated-row']:
        state['calculated-row']['partials'] = {}
    state['calculated-row']['partials'][hashkey] = partials
    state['calculated-row']['partials-file'] = 'partial-calcs'
    return state




def assemble_rows(mapping,data):
    rows = data
    if not 'include' in mapping and not 'exclude' in mapping:
        rows = filter_rows(mapping,data)
        return rows
    if 'include' in mapping:
        rows = filter_rows(mapping['include'],rows)
    if 'exclude' in mapping:
        rows = filter_rows(mapping['exclude'],rows,exclude=True)
    return rows


def filter_rows(mapping,data,exclude=False):
    fields = list(Row._fields)
    filtered = data

    for field in mapping:
        if not field in fields:
            raise Exception('unrecognized field: {}'.format(field))
        index = fields.index(field)
        target = mapping[field]
        if isinstance(target,list):
            aggregator = []
            for t in target:
                aggregator += keyword_filter(index,t,filtered)
            filtered = []
            for r in aggregator:
                if not r in filtered: filtered.append(r)
        else:
            filtered = keyword_filter(index,target,filtered)
    if exclude: matches = [r for r in data if not r in filtered]
    else: matches = filtered
    return matches

def keyword_filter(index,target,data):
    start = lambda s,t: s.startswith(t)
    end = lambda s,t: s.endswith(t)
    # generate filtering lambda
    if start(target,'*') and end(target,'*'):
        fltr = lambda r: target[1:-1] in r[index]
    elif start(target,'*'):
        fltr = lambda r: end(r[index],target[1:])
    elif end(target,'*'):
        fltr = lambda r: start(r[index],target[:-1])
    else:
        fltr = lambda r: r[index] == target
    return list(filter(fltr,data))
