#!/usr/bin/env python3
from collections import namedtuple

# Default data type to be returned by all data-acquisition
# scripts.  Key requirement for interoperability between
# various steps/components ( esp. acquisition & reshaping ).
Row = namedtuple('row',['node','sensor','unit','timestamp','value'])
