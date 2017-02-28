#!/usr/bin/env python3
import time
import csv
import os
import os.path as path

# Generate and update a simple and readable log file.
def mklog(project,text):
    hdr  = 'logtime............{}'.format(int(time.time()))
    proj = 'project............{}'.format(project)
    ftr  = '...................endlog\n'
    errdir = dirset(project)
    fpath  = dirset + 'errlog.txt'
    with open(fpath,'a') as fp:
        write = lambda txt: print(txt,file=fp)
        write(hdr)
        write(proj)
        write(text)
        write(ftr)

# write a csv of improperly formatted
# data to an appropriate errors file.
def errdata(project,data,txt='fmterr'):
    errdir = dirset(project)
    fpath = errdir + txt + '-'+ str(int(time.time())) + '.csv'
    fields = data[0]._fields
    with open(fpath,'w') as fp:
        print('writing {} malformed rows to: {}'.format(len(data),fpath))
        writer = csv.writer(fp)
        writer.writerow(fields)
        for d in data: writer.writerow(d)

# Setup error directories if necessary.
def dirset(project):
    errdir = 'tmp/errors/{}/'.format(project)
    if not path.isdir(errdir):
        os.makedirs(errdir)
    return errdir
