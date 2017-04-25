#!/usr/bin/env python3
from importlib import import_module
from src.core.data_utils import Row
from src.core.error_utils import error_template,mklog
import src.core.file_utils as ft
import os.path as path
import os

static_error = error_template('`static` data-acquisition step')

# primary entrr point: project -> config -> state -> (state,data)
def acquire(project,config,state):
    print('runnint `static` data-acquisition method...\n')
    # get contents of the `settings` field; defaults to empty dict.
    settings = config.get('settings',{})
    # get the list of parser specifications.
    parsers = config['parser']
    # generate the default directory to move successfully parsed files to.
    fmt_default = settings.get('on-fmt','tmp/archive/{}/static/'.format(project))
    # generate the default directory to move unsuccessfully parsed files to.
    err_default = settings.get('on-err','tmp/errors/{}/static/'.format(project))
    # generate the default directory to find files to parse.
    src_default = settings.get('source','tmp/inputs/{}/'.format(project))
    data = [] # collector for successufully generated rows.
    # iteratively run all parsers.
    for spec in parsers:
        name = spec['parser'] # name of perser to use.
        print('running static file parser: {}\n'.format(name))
        parser = get_parser(name) # load specified parser.
        on_fmt = spec.get('on-fmt',fmt_default) # fmt dest or default.
        on_err = spec.get('on-err',err_default) # err dest or default.
        source = spec.get('source',src_default) # file src or default.
        suffix = spec.get('suffix','*') # file suffix of targets.
        files = load_files(source,suffix) # files to parse.
        # iteratively parse all files, moving them to
        # `on_fmt` if no errors occur, and `on_err` if
        # an exception is raised by `parser`.
        for fname in files:
            try:
                print('attempting to parse file: {}'.format(fname))
                substate = state.pop(name,{})
                substate,rows = parser.parse(spec,substate,fname)
                for r in rows: assert isinstance(r,Row)
                data += rows
                if substate: state[name] = substate
                move_file(source,on_fmt,fname)
                print('{} rows acquired during parsing...\n'.format(len(rows)))
            except Exception as err:
                print('error while parsing {}: '.format(fname) + err)
                print('moving target file to: {}\n'.format(on_err))
                mklog(err)
                move_file(source,on_err,fname)
    return state,data

# move a file at src/name to dest/name.  If
# strict is false, dest will be created if
# it does not exist.
def move_file(src,dest,name,strict=False):
    mkerr = static_error('attempting to move file: ' + name)
    src = src if src.endswith('/') else src + '/'
    dest = dest if dest.endswith('/') else dest + '/'
    if not path.isdir(src):
        error = mkerr('source directory does not exist: ' + src)
        raise Exception(error)
    if not path.isdir(dest):
        if strict:
            error = mkerr('destination directory does not exist: ' + dest)
            raise Exception(error)
        else: os.makedirs(dest)
    frompath = src + name
    intopath = dest + name
    os.rename(frompath,intopath)


# attempts to load a list of files based on
# a given parser specification.
def load_files(source,suffix):
    mkerr = static_error('loading files for parsing...')
    if not path.isdir(source):
        error = mkerr('source directory does not exist: ' + source)
        raise Exception(error)
    files = fu.list_files(source)
    matches = fu.match_filetype(files,suffix)
    return matches

# attempts to load the specified parser.
def get_parser(parser):
    mkerr = static_error('loading static file parser: ' + parser)
    modname = 'src.acquire.parsers.{}'.format(parser).lower()
    try:
        mod = import_module(modname)
    except:
        error = mkerr('failed to load parser module: ' + parser)
        raise Exception(error)
    return mod
