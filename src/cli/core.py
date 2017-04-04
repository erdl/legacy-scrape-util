#!/usr/bin/env python3
import argparse as ap


def run():
    args = get_args()
    print('project-name: ',args.projectname)
    if args.acquire:
        print('acquire step: ',args.acquire)
    if args.reshape:
        print('reshape step(s): ',args.reshape)
    if args.export:
        print('export step(s): ',args.export)


def get_args():
    # initialize the parser object.
    parser = ap.ArgumentParser()
    # mandatory single-value argument defining project name.
    parser.add_argument('projectname',
        help='name of project to be created',type=str)
    # optional single-value arguemnt defining acquire step.
    parser.add_argument('-a','--acquire',
        help='acquire step to use',type=str)
    # optional mulit-value argument defining reshape step.
    parser.add_argument('-r','--reshape',
        help='reshape step to use',type=str,nargs='+')
    # optional multi-value argument defining export step.
    parser.add_argument('-e','--export',
        help='export step to use',type=str,nargs='+')
    return parser.parse_args()








# DEBUG
if __name__ == '__main__':
    run()
