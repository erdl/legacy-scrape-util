#!/usr/bin/env python3
import src.core.fileutils as fileutils
from collections import namedtuple
import toml
import os.path as path
import os
import sys

# Default data type to be returned by all data-acquisition
# scripts.  Key requirement for interoperability between
# various steps/components ( esp. acquisition & reshaping ).
Row = namedtuple('row',['node','sensor','unit','timestamp','value'])

# make a unique identifuing string from
# a node, sensor, and unit value.
def mkuid(node,sensor,unit):
    elements = (str(node),str(sensor),str(unit))
    return '-'.join(elements).lower()

# generate a uid from a given Row object.
def get_uid(row):
    if not isinstance(row,Row):
        raise Exception('invalid row type!')
    return mkuid(row.node,row.sensor,row.unit)

def get_master_config(expect=[]):
    directory = 'tmp/'
    filename = 'config'
    config = fileutils.load(directory,filename,optional=True)
    if not config:
        config = handle_no_config(directory,filename)
        missing = [e for e in expect if not e in config]
        if missing: raise Exception('project(s) not found: {}'.format(missing))
        return config
    missing = [e for e in expect if not e in config]
    if not missing: return config
    new = generate_config(directory,filename)
    new = {k: new[k] for k in new if k in missing}
    config.update(new)
    missing = [e for e in expect if not e in config]
    if missing:
        raise Exception('project(s) not found: {}'.format(missing))
    return config

def handle_no_config(directory,filename):
    print('expected master config at: ',directory)
    while True:
        rsp = input('generate master config from defaults? (y/n)')
        if rsp.lower() == 'y': break
        elif rsp.lower() == 'n':
            print('program closing...')
            sys.exit(0)
        else: print('invalid input: ',rsp)
    projdir = directory + 'projects/'
    config = generate_config(projdir)
    if not filename.endswith('.toml'):
        filename = filename + '.toml'
    with open(directory + filename,'w') as fp:
        toml.dump(config,fp)
    return config

def generate_config(projdir):
    projects = os.listdir(projdir)
    projects = [p for p in projects if path.isdir(projdir + p)]
    config = {p: {'is-active': True} for p in projects}
    return config
