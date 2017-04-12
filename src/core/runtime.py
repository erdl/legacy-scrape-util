#!/usr/bin/env python3
from importlib import import_module
import src.core.file_utils as file_utils
import src.core.error_utils as error_utils
import src.core.cli_utils as cli_utils

def run(mode='cli'):
    if mode == 'cli':
        run_cli()
    else:
        raise Exception('unrecognized mode: ' + mode)
    print('program closing...')

def run_cli():
    config = cli_utils.run_cli()
    active,inactive = sort_projects(config)
    if inactive:
        print('skipping project(s):')
        print(inactive.keys())
        print('(flagged as inactive in master config)\n')
    if not active:
        print('no active projects found...')
        return
    for project in active:
        run_wrapped(project)

# Split project from master-config into
# active and inactive sub-configurations.
def sort_projects(config):
    active = {}
    inactive = {}
    for project in config:
        projconf = config[project]
        if 'is-active' in projconf:
            if not projconf['is-active']:
                inactive[project] = projconf
        else: active[project] = projconf
    return active,inactive

# wrapper around `run project` which
# catches & logs errors.
def run_wrapped(project):
    try:
        run_project(project)
    except Exception as err:
        print('exception in {}:'.format(project))
        print(str(err) + '\n')
        error_utils.mklog(project,err)

# Handles an atomic project-run instance.
# can be called manually with project name.
def run_project(project):
    # extract conigurationa and state
    # from the appropriate project file.
    config,state = get_parameters(project)
    # check configuration for required fields.
    check_config(project,config)
    # acquire data & updated state values.
    state,data = acquire_data(project,config['acquire'],state)
    # add any generated rows.
    #data += generate_rows(data,state,config)
    # reshape the data into the desired form.
    state,data = reshape_data(project,config,state,data)
    # push data to one or more destinations.
    state = export_data(project,config,state,data)
    # update the state file w/ new values.
    update_state(project,state)

# load the config and state files.
def get_parameters(project):
    location = 'tmp/projects/{}/'.format(project)
    required = ['config']
    parameters = fileutils.load_files(location,required)
    optional = ['state']
    parameters += fileutils.load_files(location,optional)
    return parameters

# update the state file.
def update_state(project,state):
    location = 'tmp/projects/{}/'.format(project)
    fileutils.write_file(location,'state',state)


# Check config file for existence
# of any required fields.
def check_config(project,config):
    msg = 'no {} section defined for {}.'
    require = ['acquire','export']
    for req in require:
        if not req in config:
            raise Exception(msg.format(req,project))

# Acquire the data via specifid method.
# Returns data and a new state value.
def acquire_data(project,config,state):
    data = []
    for method in config:
        if not is_active(config[method]):
            print('skipping acquire method: ',method)
            print('(flagged as inactive)\n')
            continue
        scraper = get_util('acquire',config['type'])
        state,rows = scraper.scrape(project,config[method],state)
        data += rows
    print('{} rows generated during `acquire` step.'.format(len(data)))
    return state,data

# Reshape the data via specified mapping(s).
def reshape_data(project,config,state,data):
    if not data: return
    if 'reshape' in config:
        config = config['reshape']
    else: return data
    rutils = {}
    rord = []
    # assemble list of active reshape mappings.
    for kind in config:
        # ignore file specifiers.
        if 'file' in kind: continue
        if not isactive(config[kind]): continue
        rutils[kind] = get_util('reshape',kind)
        rord.append(kind)
    # sort reshape utilites by their ORD variable.
    skey = lambda k: rutils[k].ORD
    rord = sorted(rord,key=skey)
    # run all reshape utilities across data.
    for rs in rord:
        state,data = rutils[rs].reshape(project,rconf[rs],state,data)
    return state,data


# Save the data via specified channel(s).
def export_data(project,config,state,data):
    if not data:
        print('no values to export.')
        return state
    config = config['export']
    # iteratively run configured exports.
    for kind in config:
        # ignore file specifiers.
        if '-file' in kind: continue
        if not isactive(config[kind]): continue
        # load & run specified export utility.
        exutil = get_util('export',kind)
        exutil.export(data,project,config[kind])
    return state

# Generic 'utility' getter.
# Attempts to take a category & kind,
# and return a library object.
def get_util(category,kind):
    modname = 'src.{}.{}'.format(category,kind).lower()
    try: mod = import_module(modname)
    except: raise Exception('no utility at: {}'.format(modname))
    return mod

# Check a dictionary for the 'is-active'
# flag, returning true unless 'is-active'
# exists and is set to false.
def is_active(subject):
    # Check if subject is empty or False.
    if not subject: return False
    # Check if subject is simply the a
    # boolian `True`.  This is the reccommended
    # pattern if a field need hold no values, but
    # should evaluate as active.
    if isinstance(subject,bool):
        return True
    # Check for common pattern of miscellaneous
    # configuratinos livin in the `general` field.
    if 'settings' in subject:
        if not isactive(subject['general']):
            return False
    if 'is-active' in subject:
        if not subject['is-active']:
            return False
    return True
