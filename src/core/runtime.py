#!/usr/bin/env python3
from importlib import import_module
import src.core.parameters as params
import src.core.errlog as errlog

# Primary entry point for runtime.
# The one constant point in a world
# that is rife with uncertainty.
def run(project):
    # extract conigurationa and nonce
    # from the appropriate project file.
    config,nonce = params.get_parameters(project)
    # check configuration for required fields.
    check_config(project,config)
    # acquire data & updated nonce values.
    data,nonce = acquire_data(project,config,nonce)
    # reshape the data into the desired form.
    data = reshape_data(project,config,data)
    # push data to one or more destinations.
    export_data(project,config,data)
    # update the nonce file w/ new values.
    params.update_nonce(project,nonce)


# Check config file for existence
# of any required fields.
def check_config(project,config):
    msg = 'no {} method defined for {}.'
    require = ['acquire','export']
    for req in require:
        if not req in config:
            raise Exception(msg.format(req,config))

# Acquire the data via specifid method.
# Returns data and a new set of nonce values.
def acquire_data(project,config,nonce):
    dconf = config['acquire']
    # import & run the specified scraper.
    scraper = get_util('acquire',dconf['type'])
    data,nonce = scraper.scrape(project,dconf,nonce)
    print('rows acquired during scrape: {}'.format(len(data)))
    return data,nonce


# Reshape the data via specified mapping(s).
def reshape_data(project,config,data):
    if not data: return
    if 'reshape' in config:
        rconf = config['reshape']
    else: return data
    rutils = {}
    rord = []
    # assemble list of active reshape mappings.
    for kind in rconf:
        # ignore file specifiers.
        if 'file' in kind: continue
        if not isactive(rconf[kind]): continue
        rutils[kind] = get_util('reshape',kind)
        rord.append(kind)
    # sort reshape utilites by their ORD variable.
    skey = lambda k: rutils[k].ORD
    rord = sorted(rord,key=skey)
    # run all reshape utilities across data.
    for rs in rord:
        data = rutils[rs].reshape(project,rconf[rs],data)
    return data


# Save the data via specified channel(s).
def export_data(project,config,data):
    if not data:
        print('no values to export.')
        return
    exconf = config['export']
    # iteratively run configured exports.
    for kind in exconf:
        # ignore file specifiers.
        if 'file' in kind: continue
        if not isactive(exconf[kind]): continue
        # load & run specified export utility.
        exutil = get_util('export',kind)
        exutil.export(data,project,exconf[kind])

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
def isactive(subject):
    if 'is-active' in subject:
        if not subject['is-active']:
            return False
    return True
