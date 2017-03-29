#!/usr/bin/env python3
from src.core.row import Row

EXTENSIONS = {
    'calculated-row' : lambda c,s,d: generate_rows(c,s,d)
}

# primary entry point for `extra`.
def extend(config,state,data):
    for exconf in config:
        if not exconf['type'] in EXTENSIONS:
            raise Exception('unrecognized data-extension type: {}'.format(exconf['type']))
        generator = EXTENSIONS[exconf['type']]
        state,rows = generator(exconf,state,data)
        print('{} data points generated of type: {}'.format(len(rows),exconf['type']))
        data += rows
    return data,state

def generate_rows(config,state,data):
    if 'config' in config: config = config['config']
    dpc = config['data-point']
    if not 'unit' in dpc: dpc['unit'] = 'undefined'
    mkrow = lambda t,v: Row(dpc['node'],dpc['sensor'],dpc['unit'],t,v)
    addlist = assemble_rows(config['add'],data)
    if 'sub' in config:
        sublist = assemble_rows(config['sub'],data)
    else: sublist = []
    timesort,state = sort_time_points(config,state,addlist,sublist)
    rows = []
    for tindex in timesort:
        value = sum(timesort[tindex]['add']) - sum(timesort[tindex]['sub'])
        rows.append(mkrow(float(tindex),value))
    return state,rows

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
    start = lambda s,t: s.startswith(t)
    end = lambda s,t: s.endswith(t)
    for field in mapping:
        if not field in fields:
            raise Exception('unrecognized field: {}'.format(field))
        index = fields.index(field)
        target = mapping[field]
        if start(target,'*') and end(target,'*'): fltr = lambda i,r: target[1:-1] in r[i]
        elif start(target,'*'): fltr = lambda i,r: end(r[i],target[1:])
        elif end(target,'*'): fltr = lambda i,r: start(r[i],target[:-1])
        else: fltr = lambda i,r: r[i] == target
        filtered = list(filter(lambda r: fltr(index,r),filtered))
    if exclude: matches = [r for r in data if not r in filtered]
    else: matches = filtered
    return matches
