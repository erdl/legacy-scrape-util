#!/usr/bin/env python3
from src.core.runtime import run

def Main():
    projects = ['uhm-frog']
    for proj in projects:
        run(proj)

if __name__ == '__main__':
    Main()
