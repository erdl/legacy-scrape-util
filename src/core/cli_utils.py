#!/usr/bin/env python3
import src.core.file_utils as file_utils
import argparse as ap
import shutil
import sys
import os

# main entry point for the command line interface.
# currently, the cli allows for the generation of new
# project templates, as well as the selective running of
# all, or some subset, of active projects.
def run_cli():
    args = get_args()
    rslt = {}
    if not args['action']:
        print('no action specified... run with `-h` for more info.')
    elif args['action'] == 'new':
        new_project(args)
    elif args['action'] == 'run':
        rslt['projects'] = assemble_projects(args['projects'])
    else:
        print('unrecognized action: ',args['action'])
    if 'mode' in args:
        rslt['mode'] = args['mode']
    return rslt

def handle_args(args):
    args = get_args()
    if not args['action']:
        print('no action specified... run with `-h` for more info.')
        return []
    elif args['action'] == 'new':
        new_project(args)
        return []
    elif args['action'] == 'run':
        return assemble_projects(args['projects'])
    else:
        print('unrecognized action: ',args['action'])
        return []

# parses user aguments,
def get_args():
    # initialize the parser object.
    parser = ap.ArgumentParser()
    # action to be carried out.
    action = parser.add_subparsers(dest='action',
        title='action',help="desired program action (select one)")
    # parser of the `run` action.
    runproj = action.add_parser('run')
    runproj.add_argument('-p','--projects',default=['all'],
        help = 'project(s) to run',type=str,nargs='+')
    runproj.add_argument('-m','--mode',default='cli',
        choices=['cli','cron'])
    # parser for the `new` action.
    newproj = action.add_parser('new')
    newproj.add_argument('project_name',type=str,
        help='name of the new project')
    newproj.add_argument('-a','--acquire_step',default=None,
        help='acquire step to use',type=str)
    return vars(parser.parse_args())

# assemble the list of projects to be run.
def assemble_projects(projects):
    directory = 'tmp/projects/'
    allproj = file_utils.get_projects()
    if 'all' in projects:
        return allproj
    for proj in projects:
        if not proj in allproj:
            raise Exception('no project folder found matching: ' + proj)
    return projects

# generate a new project template.
def new_project(args):
    projname = args['project_name']
    acquire = args['acquire_step']
    projdir = mktemplate(projname)
    if acquire: mkacquire(projdir,acquire)
    print('new project generated at: ',projdir)

# generate an `acquire` step template to a project.
def mkacquire(projdir,steptype):
    src = 'src/core/templates/acquire/'
    dst = projdir if projdir.endswith('/') else projdir + '/'
    templates = [f for f in os.listdir(src) if f.endswith('-config.toml')]
    targets = [f for f in templates if f.startswith(steptype)]
    if not targets:
        raise Exception('no template found for: ' + steptype)
    target = targets.pop(0)
    tname = '.'.join(target.split('.')[:-1])
    shutil.copy(src + target,dst + target)
    with open(dst + 'config.toml') as fp:
        clines = fp.read().splitlines()
    for i,line in enumerate(clines):
        if line.startswith('type = '):
            clines[i] = 'type = "{}"'.format(steptype)
            clines.insert(i+1,'config-file = "{}"'.format(tname))
            break
    with open(dst + 'config.toml','w') as fp:
        for line in clines:
            print(line,file=fp)

# copy the core project template to the
# appropriate directory.
def mktemplate(projname):
    src = 'src/core/templates/project'
    dst = 'tmp/projects/{}/'.format(projname)
    shutil.copytree(src,dst)
    return dst
