#!/usr/bin/env python3
import os.path as path
import toml
import json
import os

# various utilities used for file io,
# including loading project configuration,
# state, etc...

# generate a list of all project directories.
# ignores directories with a `.` in their name.
def get_projects():
    directory = 'tmp/projects/'
    if not path.isdir(directory):
        raise Exception('expected project folder: ' + directory)
    dirlist = list_dirs(directory)
    projects = [d for d in dirlist if not '.' in d]
    return projects

# load the configuration file for a given project.
def get_config(project):
    directory = 'tmp/projects/{}/'.format(project)
    core = load_file(directory,'config')
    config = expand(directory,core)
    return config

# load existent state files if any.
def get_state(project):
    directory = 'tmp/projects/{}/state-files/'.format(project)
    if not path.isdir(directory):
        return {}
    files = list_files(directory)
    state = {}
    for f in files:
        parse = get_parser(f,strict=False)
        if not parse: continue
        name = '.'.join(f.split('.')[:-1])
        with open(directory + f) as fp:
            data = parse(fp)
        state[name] = data
    return state

# save all elements of the current state object.
def save_state(project,state):
    directory = 'tmp/projects/{}/state-files/'.format(project)
    if not path.isdir(directory):
        os.makedirs(directory)
    for key in state:
        val = state[key]
        if not val: continue
        fname = '{}.toml'.format(key)
        with open(directory + fname,'w') as fp:
            toml.dump(val,fp)

# recursively expand all `-file` fields
# within some existing dict of data.
def expand(directory,data):
    collector = {}
    # iteratively expand all `-file` fields.
    for key in data:
        if not key.endswith('-file'):
            collector[key] = data[key]
            continue
        new = '-'.join(key.split('-')[:-1])
        exp = load_file(directory,data[key])
        collector[new] = exp
    # recursively expand all dict subfields.
    for key in collector:
        val = collector[key]
        if isinstance(val,dict):
            exp = expand(directory,val)
            collector[key] = exp
    return collector

# load a target file.
def load_file(directory,target):
    files = list_files(directory)
    match = [f for f in files if f.startswith(target)]
    if not match:
        raise Exception('no file matching: ' + target)
    filename = match.pop()
    parse = get_parser(filename)
    with open(directory + filename) as fp:
        data = parse(fp)
    return data

# load a parser for some file.
def get_parser(filename,strict=True):
    if filename.endswith('toml'):
        parse = lambda fp: toml.load(fp)
    elif filename.endswith('json'):
        parse = lambda fp: json.load(fp)
    elif not strict: return None
    else: raise Exception('unknown filetype: ' + filename)
    return parse

# list all files in a directory.
def list_files(directory):
    dirlist = os.listdir(directory)
    fltr = lambda f: path.isfile(directory + f)
    return list(filter(fltr,dirlist))

# list all directories in a directory.
def list_dirs(directory):
    dirlist = os.listdir(directory)
    fltr = lambda f: path.isdir(directory + f)
    return list(filter(fltr,dirlist))

# return all elements from a list of files
# which match the specified filetype
def match_filetype(files,filetype):
    if filetype == '*': return files
    filetype = filetype.lower()
    matches = []
    for name in files:
        ext = name.split('-').pop().lower()
        if ext == filetype:
            name.append(matches)
    return matches
