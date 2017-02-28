#!/usr/bin/env python3
from collections import namedtuple

# Default data type to be returned
# by all data-acquisition scripts
Row = namedtuple('row',['node','sensor','unit','timestamp','value'])
