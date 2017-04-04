#!/usr/bin/env python3
from collections import namedtuple

# Default data type to be returned by all data-acquisition
# scripts.  Key requirement for interoperability between
# various steps/components ( esp. acquisition & reshaping ).
Row = namedtuple('row',['node','sensor','unit','timestamp','value'])


def mkuid(node,sensor,unit):
    elements = (str(node),str(sensor),str(unit))
    return '-'.join(elements).lower()

# generate
def get_uid(row):
    if not isinstance(row,Row):
        raise Exception('invalid row type!')
    return mkuid(row.node,row.sensor,row.unit)
