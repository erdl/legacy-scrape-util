#!/usr/bin/env python3
from src.core.utils import get_master_config
import argparse as ap
import shutil
import os


def run():
    args = vars(get_args())
    if not args['action']:
        print('no action specified... run with `-h` for more info.')
    elif args['action'] == 'run':
        run_projects(args)
    elif args['action'] == 'new':
        new_project(args)
    else:
        print('unrecognized action: ',args['action'])

def run_projects(args):
    projects = args['projects']
    if 'all' in projects:
        config = get_master_config()
        projects = [p for p in config]
    else: config = get_master_config(expect=projects)
    # CONTINUE HERE
    for proj in projects:
        if isinstance(config[proj],dict):
            if 'is-active' in config[proj]:
                if not config[proj]['is-active']:
                    print('skipping {} (flagged as inactive)'.format(proj))





def new_project(args):
    projname = args['project_name']
    acquire = args['acquire_step']
    projdir = mktemplate(projname)
    if acquire: mkacquire(projdir,acquire)
    print('new project generated at: ',projdir)


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
    # parser for the `new` action.
    newproj = action.add_parser('new')
    newproj.add_argument('project_name',type=str,
        help='name of the new project')
    newproj.add_argument('-a','--acquire_step',default=None,
        help='acquire step to use',type=str)
    return parser.parse_args()


def mkacquire(projdir,steptype):
    src = 'src/cli/templates/acquire/'
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


def mktemplate(projname):
    src = 'src/cli/templates/project'
    dst = 'tmp/projects/{}/'.format(projname)
    shutil.copytree(src,dst)
    return dst




# DEBUG
if __name__ == '__main__':
    run()
