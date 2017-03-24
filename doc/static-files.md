# Static Files

Methods of [data acquisition](./data-acquisition.md) which
read from static files end up sharing a decent amount of
generic boilerplate.  For this reason, special `static`
data-acquisition methods can be created.


## Overview

The `static` acquisition method supports the rapid development
and standardization of file parsers by providing a simple
interface by which new parsers can be added, and forcing
a set of unified behaviors on all parsers.  What we may lose in
potential 'optimizations' by doing this, we more than make up for
by greatly simplifying configuration and troubleshooting across
file parsing implementations.


## Example Config

```toml
# example `acquire` section, using the `static`
# acquisition method, with the `hoboware` parser.

[acquire]
type = "static"
suffix = "csv"
source = "path/to/csv/files/" # optional

[acquire.parser]
type = "hoboware"
# args to be passed to the parser go here.

[acquire.moveto] # optional section
fmt = "move/here/on/success/"
err = "move/here/on/failure/"
```

## Boilerplate

```python
#!/usr/bin/env python3
from src.core.row import Row

# List required configuration fields if any.
REQUIRE = []

# Entry-point called by `static`.  Arguments will
# be a dictionary consisting of any keys/values
# found in [acquire.parser], and a path which
# points to the file in need of conversion.
def parse(config,filepath):
    # do all the things...
    return rows
```


## Development Notes

Need to decide if the parser implementation
gets passed a file path or a file pointer...
Current implementation uses file path.
