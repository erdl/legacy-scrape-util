#!/usr/bin/env python3
from importlib import import_module
from src.core.data_utils import Row
import os.path as path
import os

# primary entry point.
def scrape(project,config,state):
    source = dircheck(project,config)
    parser = get_parser(config['parser'])
    ext = '.{}'.format(config['settings']['suffix'])
    files = [ f for f in os.listdir(source) if f.endswith(ext) ]
    if not files:
        print('no files found at: {}'.format(source))
        return [],{}
    rows,fmts,errs = [],[],[]
    parse = lambda f: parser.parse(project,config['parser'],state,source+f)
    for f in files:
        try:
            state,r = parse(f)
            rows += r
            fmts += f
        except Exception as err:
            mklog(project,err)
            errs += f
    move_originals(project,config,source,fmts,errs)
    return state,rows


# check that the input directory exists.
def dircheck(project,config):
    default = 'tmp/inputs/{}/'.format(project)
    if 'source' in config['settings']:
        source = config['settings']['source']
    else:
        print('assuming default directory as source...')
        source = default
    if not path.isdir(source):
        if source == default:
            msg = 'missing default directory: {}'.format(source)
        else: msg = 'directory does not exist: {}'.format(source)
        raise Exception(msg)
    return source


# attempts to load the specified parser.
def get_parser(config):
    if not 'type' in config:
        raise Exception('no parser type defined.')
    pname = config['type']
    modname = 'src.acquire.parsers.{}'.format(pname).lower()
    try: mod = import_module(modname)
    except: raise Exception('no parser named: {}'.format(pname))
    return mod

# handles relocation of files after parser is run.
def move_originals(project,config,source,fmts,errs):
    if 'move-to' in config: moveto = config['move-to']
    else: moveto = {}
    if 'fmt' not in moveto: moveto['fmt'] = 'default'
    if 'err' not in moveto: moveto['err'] = 'default'
    reloc = lambda m,f,k : relocate(project,source,m,f,k)
    if fmts: reloc(moveto['fmt'],fmts,'archive')
    if errs: reloc(moveto['err'],errs,'errors')

# performs a specified batch relocation.
# supports three key-word actions:
# `default`, `delete`, and `rename`.
def relocate(project,source,moveto,files,kind):
    if moveto == 'default':
        moveto = 'tmp/{}/{}/static/'.format(kind,project)
    elif moveto == 'delete':
        for f in files: os.remove(source+f)
        return
    elif moveto == 'rename':
        files = [ f + '.' + kind for f in files ]
        moveto = source
    dirset(moveto)
    print('moving {} files to {}...'.format(len(files),moveto))
    for f in files:
        frompath = source + f
        intopath = moveto + f
        os.rename(frompath,intopath)

# ensure that a directory exists.
def dirset(directory):
    if not path.isdir(directory):
        os.makedirs(directory)
