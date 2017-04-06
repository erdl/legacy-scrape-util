#!/usr/bin/env python3
import argparse as ap
import shutil
import os


def run():
    args = get_args()
    projdir = mktemplate(args.projectname)
    if args.acquire:
        generate_acquire_step(projdir,args.acquire)
    print('project template generated at: ',projdir)


def get_args():
    # initialize the parser object.
    parser = ap.ArgumentParser()
    # mandatory single-value argument defining project name.
    parser.add_argument('projectname',
        help='name of project to be created',type=str)
    # optional single-value arguemnt defining acquire step.
    parser.add_argument('-a','--acquire',
        help='acquire step to use',type=str)
    return parser.parse_args()


def generate_acquire_step(projdir,steptype):
    src = 'src/cli/templates/acquire/'
    dst = '{}config/'.format(projdir)
    templates = [f for f in os.listdir(src) if f.endswith('-config.toml')]
    targets = [f for f in templates if f.startswith(steptype)]
    if not targets:
        raise Exception('no template found for: ' + steptype)
    target = targets.pop(0)
    tname = '.'.join(target.split('.')[:-1])
    shutil.copy(src + target,dst + target)
    with open(dst + 'config-core.toml') as fp:
        clines = fp.read().splitlines()
    for i,line in enumerate(clines):
        if line.startswith('type = '):
            clines[i] = 'type = "{}"'.format(steptype)
            clines.insert(i+1,'config-file = "{}"'.format(tname))
            break
    with open(dst + 'config-core.toml','w') as fp:
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
