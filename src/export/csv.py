#!/usr/bin/env python3
from src.core.errlog import errdata
import os.path as path
import time
import csv
import os

# Main save-to-csv entry point.
def save(data,project,csv_config,ftxt='raw'):
    # TODO: use csv config values for something...
    # probably for choosing file txt, etc...
    save_csv(data,project,ftxt)

# Save it to a thing!
def save_csv(data,project,ftxt):
    projdir = dirset(project)
    fname = fset(ftxt)
    fpath = projdir + fname
    fields = data[0]._fields
    print('writing {} rows to {}'.format(len(data),fpath))
    with open(fpath,'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(fields)
        for d in data: writer.writerow(d)

# set up & return project output dir.
def dirset(project):
    projdir = 'tmp/outputs/{}/'.format(project)
    if not path.isdir(projdir):
        os.makedirs(projdir)
    return projdir

# set up & return file name.
def fset(ftxt):
    ft = str(int(time.time()))
    fname = '{}-{}.csv'.format(ftxt,ft)
    return fname
