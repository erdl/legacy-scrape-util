# Module Development

This document contains guidelines and boilerplate for developing new
modules for the utility.


## Overview

Modules come in three flavors, depending on wether they are for data acquisition,
reshaping, or exporting.  All modules are imported by name at runtime, and executed via
a specific function exposed by all modules of that type.  In order to implement a module,
simply add a python file with the desired module name as its filename to the appropriate
directory, and wrap the module's functionality in the appropriate function interface.

Among other things, all modules are passed a `state` variable, and are expected to return
a `state` variable as well.  This variable is a dictionary whose contents are saved
between runs, allowing modules to maintain persistent state if needed.  Each module has
access only to its own `state` variable, allowing modules to save data under generic keys
without fear of collisions.  The `state` variable is commonly used for storing nonce values
and/or partially generated calculated fields.


## Data-Acquisition Modules

Data acquisition modues should be placed in the [`src/acquire/`](../src/acquire)
directory.  Each `.py` file in this directory is expected to be built upon the
following boilerplate:

```python
from src.core.data_utils import Row

def acquire(project,config,state):
    # module logic goes here...
    return state,data

```

### Arguments

The `project` variabe is a string representing the name of the
current project.  Many data-acquisition modules use this as the default value
for `node` in rows for which no `node` value is defined.  The `config` and `state`
variables are both dictionaries.  The `config` dict contains all user-supplied
configurations.  The `state` dict will contain values, if any, which were returned
in the state dict of the previous run of the module.

### Returns

The `state` variable should be a dict of any values that the implementer
would like to have accessible the next time this module is run (*Ex:* nonce values
for a web scraping module).  The `state` dict must contain only valid `toml` data-types.
These include dicts, strings, floats, integers, and arrays (a python list consisting of
a single data-type will suffice here).  The `data` variable must be a list of valid `Row`
objects, representing the data-points acquired during this step.


## Data-Reshaping Modules

Data reshaping modules should be placed in the [`src/reshape/`](../src/reshape)
directory.  Each `.py` file in this directory is expected to be built upon the
following boilerplate:

```python
ORD = ...

def reshape(project,config,state,data):
    # module logic goes here...
    return state,data
```

The `ORD` variabe should be an integer value representing the modules position
within the order of reshaping operations.  Unlike the data-acquisition step, the
order of operation is very important for reshaping.  For example, the `value` reshaping
module is always called before the `field` reshpaing module. This ensures that all
`value` configs are portable across implementations, as they can be written assuming
that the generic field structure of `(node,name,unit,timestamp,value)` is still intact.
It also prevents minor changes in the `field` step from breaking the `value` implementation.

The arguments and returns of this functiona are functionally identicle to those of
the data-acquisition step, with the one exception being the addition of the `data`
argument, which is a list of `Row` objects which are to be the subject of the
reshaping. 


## Data-Exporting Modules

Data exporting modules should be placed in the [`src/export/`](../src/export)
directory.  Each `.py` file in this directory is expected to be build upon the
following boilerplate:

```python
def export(project,config,state,data):
    # module logic goes here...
    return state
```

Notice that the `export` function expects the same arguments as the `reshape` function,
but only returns a `state` variable.  Export modules are allowed to modify data
internally if needed, but they do not pass changes back to the rest of the pipeline.


