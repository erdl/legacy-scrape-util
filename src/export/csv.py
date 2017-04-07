#!/usr/bin/env python3
from src.core.error_utils import errdata,mklog
import os.path as path
import time
import csv
import os

# Main save-to-csv entry point.
def export(data,project,config):
    # TODO: use csv config values for something...
    # probably for choosing file txt, etc...
    try: save_csv(project,data)
    except Exception as err:
        mklog(project,err)
        errdata(project,data,txt='csverr')

# Save it to a thing!
def save_csv(project,data,txt='raw'):
    projdir = dirset(project)
    fname = fset(txt)
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
def fset(txt):
    ft = str(int(time.time()))
    fname = '{}-{}.csv'.format(txt,ft)
    return fname
