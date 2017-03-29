#!/usr/bin/env python3
import toml
import json
import os

# supported filetypes.
FILETYPES = ['json','toml']

# Primary entry point for loading files.
def load_files(directory,targets):
    configs = []
    for target in targets:
        configs.append(load(directory,target))
    return configs

# primary entry-point for writing configuration files.
def write_file(directory,target,data,filetype='toml'):
    # check for case of improper filetype.
    if not filetype in FILETYPES:
        raise Exception('unsupported filetype: {}'.format(filetype))
    # check for case of nested directories.
    cdir = directory + target + '/'
    if os.path.isdir(cdir):
        directory = cdir
        target = target + '-core'
    # call recursive writer to ensure subdividing
    # and separate saving of fields with `-file` declarations.
    recursive_write(directory,data,filetype,target=target)

# Top-level loading function.
# If directory matching target exists,
# assumes form dir/target/target-core.something.
def load(directory,target):
    cdir = directory + target + '/'
    if os.path.isdir(cdir):
        directory = cdir
        target = target + '-core'
    files = list_files(directory)
    fname = get_filename(files,target)
    fcore = read_file(directory + fname)
    return expand(fcore,directory,files)

# Recursively expand any '-file' fields.
def expand(config,directory,files):
    newfields = {}
    for key in config:
        # field names should not start with '-'.
        if key.startswith('-'):
            raise Exception('invalid field name: {}'.format(key))
        # iteratively expand all sub-fields.
        if isinstance(config[key],dict):
            config[key] = expand(config[key],directory,files)
            continue
        # iteratively expand lists of dicts.
        if isinstance(config[key],list):
            for i,item in enumerate(config[key]):
                if isinstance(item,dict):
                    config[key][i] = expand(item,directory,files)
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
        # Save the contents of the specified file to
        # corresponding field, ommitting `-file`.
        keyname = '-'.join(key.split('-')[:-1])
        keydata = read_file(directory + fname)
        newfields[keyname] = keydata
    config.update(newfields)
    return config

# Expand a 'file-string' to its actual filename.
def get_filename(files,fstring):
    # The `fstring` may be user-defined values due to `expand` step,
    # so gotta make sure that the user actually supplied a string.
    if not isinstance(fstring,str):
        raise Exception('expected a filename, but got: {}'.format(fstring))
    # Case-sensitivity is annoying.
    fstring = fstring.lower()
    # Reformat a filename to remove case & trailing extension.
    fmt = lambda f: '.'.join(f.split('.')[:-1]).lower()
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
    # get the reader function.
    read = get_reader(filepath)
    # get contents with acquired reader.
    with open(filepath) as fp:
        data = read(fp)
    return data

# acquire the appropriate reader function.
def get_reader(filename):
    # dict of readers by file-type.
    # currently maintained separately from
    # the FILETYPES variable, as they may not
    # be a one-to-one mapping between the two
    # at some point in the future.
    readers = {
        'json' : lambda fp: json.load(fp),
        'toml' : lambda fp: toml.load(fp)
    }
    # Get a valid reader for the file.
    # Previous checks already cover the possibility
    # of an invalid filetype, so assume valid here.
    check = lambda r: filename.endswith(r)
    valid = list(filter(check,readers.keys())).pop(0)
    return readers[valid]

def get_writer(filetype):
    writers = {
        'json' : lambda data,fp: json.dump(data,fp=fp,indent=2),
        'toml' : lambda data,fp: toml.dump(data,fp)
    }
    return writers[filetype]

# recursively sub-divides the data based on `-file` declarations,
# writing each subset of data to an appropriately named file.
def recursive_write(directory,data,filetype,target=None):
    # trim trailing word of string.
    trim = lambda s: '-'.join(s.split('-')[:-1])
    # collect keys associated with '-file' fields.
    subfiles = [(trim(k),k) for k in data if k.endswith('-file')]
    for sd,st in subfiles:
        # target name associated with `-file` declaration.
        subtarget = data[st]
        # data associated with `-file` declaration.
        subdata = data[sd]
        # recursively write subdata to subtarget.
        recursive_write(directory,subdata,filetype,target=subtarget)
        # remove subdata field to prevent writing it to two locations.
        del data[sd]
    # iteratively write & remove `-file` declarations in nested dicts.
    for k in data:
        if isinstance(data[k],dict):
            data[k] = recursive_write(directory,data[k],filetype)
    # if no target is defined, we are in a recursive step
    # and must return our data so as to propogate deleted fields.
    if not target:
        return data
    # write remaining contents of data to target.
    tpath = directory + target + '.' + filetype
    execute_write(tpath,data,filetype)

# execute the actual write step.
def execute_write(filepath,data,filetype):
    write = get_writer(filetype)
    with open(filepath,'w') as fp:
        write(data,fp)
