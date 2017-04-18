#!/usr/bin/env python3
from src.core.error_utils import errdata, error_template
import src.core.data_utils as du

# Determines order of application
# during mulit-phase reshaping process.
ORD = 0

reshape_error = error_template('`value` based data-reshaping step')


# reshape : proj -> conf -> state -> data -> (state,data)
def reshape(project,config,state,data):
    settings = config['settings']
    actions = ['filter','generate','replace']
    if 'in-order' in settings:
        order = settings['in-order']
    else: order = [a for a in actions if a in config]
    errors = []
    for action in order:
        substate = state[action] if action in state else {}
        if action == 'filter':
            substate,data = run_filters(project,config,substate,data)
        elif action == 'generate':
            substate,data = run_generators(project,config,substate,data)
        elif action == 'replace':
            substate,data = run_replacements(project,config,substate,data)
        else: raise Exception('unknown action: ' + action)
        if substate: state[action] = substate
        else: state.pop(action,None)
    return state,data

# Filter out undesired data-points, either
# by name of data-point, or by value range.
def run_filters(project,config,state,data):
    settings = config['settings']
    filters = config['filter']
    binary = {k: v for k,v in filters.items() if isinstance(v,bool)}
    limits = {k: v for k,v in filters.items() if isinstance(v,dict)}
    uidsort = sort_by_uid(settings,data)
    removed = []
    for uid in binary:
        if binary[uid] is False:
            removed += uidsort.pop(uid,())
    for uid in limits:
        if not uid in uidsort: continue
        rows = uidsort[uid]
        if 'dec' in limits[uid]:
            places = limits[uid]['dec']
            if places == 0:
                fltr = lambda r: int(round(r,places))
            else: fltr = lambda r: round(r,places)
            rows = du.map_rows(fltr,'value',rows)
        if 'max' in limits[uid]:
            fltr = lambda v: v <= limits[uid]['max']
            rows,remove = du.split_rows(fltr,rows,target='value')
            removed += remove
        if 'min' in limits[uid]:
            fltr = lambda v: v >= limits[uid]['min']
            rows,remove = du.split_rows(fltr,rows,target='value')
            removed += remove
        uidsort[uid] = rows
    filtered = []
    for rows in uidsort.values():
        filtered += rows
    handle_removals(project,config,'filter',removed)
    return state,filtered


# Generate one or more new sets of data-points
# by adding and subtracting `value` fields.
def run_generators(project,config,state,data):
    generators = config['generate']
    # check generators for required fields, and
    # supply any needed default values.
    generators = check_generators(project,generators)
    # get any partially collected data from previous iteration.
    partials = state.pop('partials',{})
    # get a uid generator.
    mkuid = du.get_uid_generator()
    # initialize collectors for partials & generated rows.
    newpartials,newrows = {},[]
    for gen in generators: # iteratively run all generators.
        node,name,unit = gen['node'],gen['name'],gen['unit']
        # generate a type-checked row generator.
        mkrow = du.row_generator(node,name,unit)
        gid = mkuid(mkrow(0,0)) # get the uid of the generated rows.
        acount = len(gen['add']) # expected data-point count for `add`.
        scount = len(gen['sub']) # expected data-point count for `sub`.
        sort = {} # { timestamp: {action: { uid: value } } }
        for row in data: # iterate over data to find & sort matches.
            uid = mkuid(row) # get the row uid.
            if uid in gen['add']:
                action = 'add'
            elif uid in gen['sub']:
                action = 'sub'
            else: continue
            tid = str(int(row.timestamp)) # generate time id.
            if not tid in sort: # initialize new time point if needed.
                sort[tid] = {'add': {}, 'sub': {}}
            sort[tid][action][uid] = row.value
        if gid in partials: # get any partials if they exist.
            for tid in sort:
                if not tid in partials[gid]: continue
                pa = partials[gid][tid].get('add',{})
                ps = partials[gid][tid].get('sub',{})
                sort[tid]['add'].update(pa)
                sort[tid]['sub'].update(ps)
        # iterate though the fully populated sorting,
        # generating calculated data-points.
        for tid in sort:
            ac = len(sort[tid]['add']) # `add` count.
            sc = len(sort[tid]['sub']) # `sub` count.
            # If one or more data-points are still missing,
            # save the collected data to `newpartials`.
            if not ac == acount or not sc == scount:
                if not gid in newpartials:
                    newpartials[gid] = {}
                newpartials[gid][tid] = sort[tid]
                continue
            # calculate value of new row/data-point.
            val = sum(sort[tid]['add'].values())
            val -= sum(sort[tid]['sub'].values())
            row = mkrow(tid,val) # pass time & value to generator.
            newrows.append(row)
    # save partials to `sate` if appropriate.
    if newpartials:
        state['partials'] = newpartials
        state['partials-file'] = 'partial-calcs'
    data += newrows
    return state,data



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
def run_replacements(project,config,state,data):
    replacements = {du.fmt_string(k): v for k,v in config['replace'].items()}
    settings = config.get('settings',{})
    uidsort = sort_by_uid(settings,data)
    target = settings.get('to-replace',None)
    remapped = []
    for uid in replacements:
        umap = replacements[uid]
        if target: umap = { target: umap }
        rows = uidsort.pop(uid,[])
        remap = lambda row: du.update_row(umap,row)
        newrows = list(map(remap,rows))
        remapped += newrows
    # if any rows remain in uidsort, pass off to handler.
    if uidsort:
        unmapped = []
        for rows in uidsort.values():
            unmapped += rows
        handle_removals(project,config,'replace',unmapped)
    return state,remapped



def sort_by_uid(settings,rows):
    key = settings.get('uid-key',None)
    mkuid = du.get_uid_generator(key)
    uidsort = {}
    for row in rows:
        uid = mkuid(row)
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

def handle_removals(project,config,step,rows):
    if not rows: return
    key = 'on-{}'.format(step)
    if not key in config['settings']: return
    action = config['settings'][key]
    if action == 'discard': return
    elif action == 'archive':
        du.save_archive(project,step,rows)
    elif action == 'error':
        errdata(project,rows,txt = step + 'err')
    else:
        raise Exception('unrecognized action: ' + action)
