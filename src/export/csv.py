#!/usr/bin/env python3
from src.core.error_utils import errdata,mklog
import os.path as path
import time
import csv
import os

# Main save-to-csv entry point.
def export(project,config,state,data):
    if not data: return
    # if `settings` exists, get it.
    settings = config.get('settings',{})
    # generate filepath & initialize destination
    # directory if it does not yet exist.
    filepath = setup(project,settings)
    # save all data to the target file.
    save_csv(filepath,data)
    return state


# load destination directory, generating it if needed.
# generate file name based on `file-spec`.
# return full path to file to target file.
def setup(project,settings):
    dest = settings.get('directory','tmp/outputs/{}/'.format(project))
    dest = dest if dest.endswith('/') else dest + '/'
    if not path.isdir(dest):
        os.makedirs(dest)
    spec = settings.get('file-spec',{})
    tag,stamp = spec.get('tag','export'),spec.get('timestamp',True)
    name = tag + '-' + str(int(time.time())) if stamp else tag
    filepath = dest + name + '.csv'
    return filepath

# saves all rows to the file specified by `filepath`.
# automatically infers headers from the `_fields` method
# of the `namedtuple` object.
def save_csv(filepath,rows):
    if not rows: return
    fields = rows[0]._fields
    print('writing {} rows to {}'.format(len(rows),filepath))
    with open(filepath,'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(fields)
        for row in rows:
            writer.writerow(row)
