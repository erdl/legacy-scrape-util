#!/usr/bin/env python3
from importlib import import_module
from src.core.row import Row
import os.path as path
import os

# primary entry point.
def scrape(project,config,nonce):
    parser,source = setup(project,config)
    ext = '.{}'.format(config['suffix'])
    files = [ f for f in os.listdir(source) if f.endswith(ext) ]
    if not files:
        print('no files found at: {}'.format(source))
        return [],{}
    rows,fmts,errs = [],[],[]
    parse = lambda f: parser.parse(config['parser'],source+f)
    for f in files:
        try:
            rows += parse(f)
            fmts += f
        except Exception as err:
            mklog(project,err)
            errs += f
    movefiles(project,config,source,fmts,errs)
    return rows,nonce

# get perser and source directory while
# performing all necessary pre-checks.
def setup(project,config):
    # check for required configuration fields.
    fieldcheck(config,['parser','suffix'])
    # check & extract the source directory.
    source = dircheck(project,config)
    # acquire the parser.
    parser = get_parser(config['parser'])
    # check parser config for required fields.
    fieldcheck(config['parser'],parser.REQUIRE)
    # return the acquired parser.
    return parser,source

# check that the input directory exists.
def dircheck(project,config):
    default = 'tmp/inputs/{}/'.format(project)
    if 'source' in config:
        source = config['source']
    else:
        print('assuming default directory as source...')
        source = default
    if not path.isdir(source):
        if source == default:
            msg = 'missing default directory: {}'.format(source)
        else: msg = 'directory does not exist: {}'.format(source)
        raise Exception(msg)
    return source

# iteratively check a dictionary for
# a list of required fields.
def fieldcheck(config,required):
    for req in required:
        if req not in config:
            raise Exception('missing required field: {}'.format(req))

# attempts to load the specified parser.
def get_parser(pconfig):
    if not 'type' in pconfig:
        raise Exception('no parser type defined.')
    pname = pconfig['type']
    modname = 'src.acquire.parsers.{}'.format(pname).lower()
    try: mod = import_module(modname)
    except: raise Exception('no parser named: {}'.format(pname))
    return mod

# handles relocation of files after parser is run.
def movefiles(project,config,source,fmts,errs):
    if 'moveto' in config: moveto = config['moveto']
    else: moveto = {}
    if 'fmt' not in moveto: moveto['fmt'] = 'default'
    if 'err' not in moveto: moveto['err'] = 'default'
    reloc = lambda m,f,k : relocate(project,source,m,f,k)
    if fmts: reloc(moveto['fmt'],fmts,'outputs')
    if errs: reloc(moveto['err'],errs,'errors')

# performs a specified batch relocation.
# supports three key-word actions:
# `default`, `delete`, and `rename`.
def relocate(project,source,moveto,files,kind):
    if moveto == 'default':
        moveto = 'tmp/{}/{}/static-raw/'.format(kind,project)
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
