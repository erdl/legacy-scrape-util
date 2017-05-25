# The `tmp/` Directory

The `tmp/` directory is the default location for all non-source files.
The utility will automatically search this directory for project
specifications, and will save errors, file outputs, and archives
within this directory by default.  Data-acquisition methods which
operate on static files also take their inputs from this directory.


## Overview

The a directory tree is shown below representing the various.
defaults assumed by the program.  All directories and their contents
are auto-generated on an as-needed basis unless otherwise stated.

```
.
|___projects/
|   |___project-name/
|   |   |___config.toml
|   |   |...
|   |...
|___inputs/
|   |...
|___outputs/
|   |...
|___errors/
|   |...
|___archive/
|   |...
|...

``` 

Upon startup, the utility searches `tmp/projects/`, treating each
enclosed directory as an independent project specification.  Each
project specification must, at a minimum, contain a single configuration
file name `config.toml`, which specifies the actions to be taken when
running the project.

The `static` data-acquisition method, which encompases various parsers
for static files, defaults to searching `tmp/inputs/project-name/` for
files to parse.  

The `outputs`, `errors`, and `archive` directories are all auto-generated
as they are required.  As their names imply, these directories contain
finalized output data, erroneous data, and data that has been archived
for auditing or other purposes, respecitvely.


