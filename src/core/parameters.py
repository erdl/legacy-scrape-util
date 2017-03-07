#!/usr/bin/env python3
import os.path as path
import time
import json
import os

# Loage gauges and nonce.
def get_parameters(project):
    # check that project folder exists.
    check_project(project)
    # load configuration from project folder.
    config = get_config(project)
    # load nonce from project folder.
    nonce = get_nonce(project)
    return config,nonce

# Check if project file exists
def check_project(project):
    projects = os.listdir('tmp/projects/')
    if not project in projects:
        raise Exception('no project named {}'.format(project))

# get project configuration
def get_config(project):
    fname = 'config.json'
    # load core configuration file.
    config = get_projfile(project,fname)
    if not config:
        raise Exception('no config found for {}'.format(project))
    # expand any file specifiers in configuration.
    config = expand_config(project,config)
    return config

# Get nonce if exists, or return {}
def get_nonce(project):
    fname = 'nonce.json'
    nonce = get_projfile(project,fname)
    if not nonce:
        nonce = {}
    return nonce

# Generic method to safely load a json
# encoded file from a project directory.
# Returns `None` if no such file exists.
def get_projfile(project,fname):
    fpath = 'tmp/projects/{}/{}'.format(project,fname)
    if not '.json' in fpath:
        fpath += '.json'
    if path.isfile(fpath):
        with open(fpath) as fp:
            data = json.load(fp)
    else: data = None
    return data

# Expand any `-file` keys in config
def expand_config(project,config):
    new = {}
    for c in config:
        cfield = config[c]
        new[c] = {}
        for k in cfield:
            # act only on fields ending in '-file'.
            if not '-file' in k: continue
            n = k.split('-').pop(0)
            # treats value as name of file to be loaded.
            d = get_projfile(project,cfield[k])
            if d: new[c][n] = d
            else: raise Exception('no file named {}'.format(cfield[k]))
    # expand configuration w/ loaded files.
    for f in new:
        config[f].update(new[f])
    return config

# Update the nonce file with new value(s).
def update_nonce(project,nonce):
    check_project(project)
    fpath = 'tmp/projects/{}/nonce.json'.format(project)
    with open(fpath,'w') as fp:
        json.dump(nonce,fp)
