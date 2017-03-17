#!/usr/bin/env python3
from src.core.errlog import mklog
import toml
import json
import os

# supported filetypes.
FILETYPES = ['json','toml']

# Primary entry point.
def get_files(project,directory,targets):
    files = list_files(directory)
    fdata = []
    for fname in targets:



# Recursively expand any '-file' fields.
def expand(config,directory,files):
    for key in config:
        # field names should not start with '-'.
        if key.startswith('-'):
            raise Exception('invalid field name: {}'.format(key))
        # iteratively expand all sub-fields.
        if isinstance(config[key],dict):
            config[key] = expand(config[key],directory,files)
            continue
        # If key is not of form 'fieldname-file', we're done here.
        if not '-file' in key:
            continue
        # Get value of `-file` field.
        fstring = config[key]
        # The `-file` field must be a valid string.
        if not isinstance(fstring,str):
            raise Exception('`-file` field must be a string: {}'.format(key))
        # Expand fstring to full file name.
        fname = get_filename(files,fstring)
        # Check if `get_filename`.
        if not fname:
            raise Exception('no file found for key: {}'.format(key))
        # Save the contents of the specified file to
        # corresponding field, ommitting `-file`.
        keyname = '-'.join(key.split('-')[:-1])
        keydata = read_file(directory + next(valid))
        config[keyname] = keydata
    return config

# Expand a
def get_filename(files,fstring):
    # The `fstring` may be user-defined values due to `expand` step,
    # so gotta make sure that the user actually supplied a string.
    if not isinstance(fstring,str):
        raise Exception('expected a filename, but got: {}'.format(fstring))
    # Case-sensitivity is annoying.
    fstring = fstring.lower()
    # Reformat a filename to remove case & trailing extension.
    fmt = lambda f: '.'join(f.split('.')[:-1]).lower()
    # Format a filename & check against fstring.
    check = lambda f: fmt(f) == fstring
    # Compile list of matching files.
    valid = list(filter(check,files))
    # If no matching files, something has gone horribly horribly wrong!
    if not valid:
        raise Exception('no file found matching: {}'.format(fstring))
    # Return a matching file name.
    return valid.pop(0)


# get all valid files in directory.
def list_files(directory):
    # checks to see if f is a supported filetype
    check = lambda f: any(map(lambda t: f.endswith(t),FILETYPES))
    # generate list of files of supported types
    files = [ f for f in os.listdir(directory) if check(f)]
    return files

# read contents of target file.
def read_file(filepath):
    read = get_reader(filepath)
    with open(filepath) as fp:
        data = read(fp)
    return data

# acquire the appropriate reader function.
def get_reader(filename):
    readers = {
        'json' : lambda fp: json.load(fp),
        'toml' : lambda fp: json.load(fp)
    }
    check = lambda r: filename.endswith(r)
    valid = filter(check,readers.keys())
    return readers[valid.next()]
