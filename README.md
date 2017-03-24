# scrape-util

## Gameplan

This whole 'make a new suite for every sensor scraping task'
thing is getting out of hand.  We need one clear and concise
application that handles it all, with configurable features
and a modular architecture.  We have the technology!

## Ideas/Notes

We can break up the common elements of all scraping tasks fairly
easily with this data model:

- *Project:* A named collection of configurations that relate.
- *Node:* A named entity that one or more sensors relate to
(logger, building, etc...).
- *Sensor:* The named entity for which a given stream of
data has a one-to-one mapping.
- *Unit:* The units encoded by the sensor's readings.
- *Timestamp:* The time of the specific reading in question.
- *Value:* The reading its self.

The project will be the named instance over which a set of
configurations apply.  As such, a project can be a single persistent
variable across a given cycle of the application.  All remaining
fields are fields for which more than one value may exist within a single
set of data, and as such they should be logged on a
per instance basis for clarity and simplicity.
Unit and Sensor will presumably change many times across
a single project instance, but presumably vary together.
Timestamp and value should be unique to a given data point.

Next, we can break up the common phases of an application cycle into
a series of 'black-box' entities like so:

- *Data Acquisition:* The scrape portion of the cycle.
- *Data Reshaping:* The mapping portion of the cycle.
- *Data Logging:* The storage portion of the cycle.

By standardizing the output of of the Data Acquisition phase
to the above data model, we can greatly simplify the data reshaping
step, by giving all reshaping configurations a common grammar.
Since the Data Logging step is separated from any direct data
manipulation, we can reasonably allow all exported data to infer
fields and shape based upon the output of the reshaping step.

## Temporary Files

Any data which is not a hard-coded feature (config files,
CSVs, error logs, etc...) will live inside the `tmp` directory.
Sub-directories will segregate data by kind and project.  The only
required directory for a given project to be run should be
`tmp/projects/projectname/`.  This will be where the project's
configuration files will live, along with any other resources that
the project cannot generate at runtime.  Two other
sub-sections of `tmp` are currently defined: `outputs` and `errors`.
Both of these file structures are generated at runtime if and when
they are needed.  The map below depicts the file system
as it is currently specified:

````
.
|___tmp
    |___projects
    |   |___projname
    |   |   |___config.toml
    |   |   |___state.toml
    |   |   |...
    |   |...
    |___outputs
    |   |___projname
    |   |   |...
    |   |...
    |___errors
    |   |___projname
    |   |   |__log.txt
    |   |   |...
    |   |...
    |...

````

## Configuration Files

The heart of this project is the configuration file.
All configuration files live in `tmp/projects/projectname/`.
The main configuration file will be named `config.json`.
Any configuration field which is sufficiently verbose to
require its own configuration file can be linked to by
appending `-file` to the field name, and having the value
be the name of the file (the `.json` extension can be left off).
For example, if we wish to have our `export` field imported from
a file named `export-settings.json`, we can replace the `export` field
in our main `config.json` file with `"export-file" : "export-settings"`.

Configuration files are the main way we tell the program what we
want done.  Configuration files can be in `json` or `toml` format.
The `toml` format is generally preferred because it tends to be
more readable, but do what comes naturally.  Configuration files
can be quite complex when one's needs are complex, but they can
be quite simple as well:

```toml
[acquire]
type = "egauge"

[acquire.gauges]
some-gauge-id = 000

[export]
csv = true

```

The `type = "egauge"` field indicates that we want to scrape data
from the egauge API.  Each type of data-acquisition defines its
own sub-sections which it requires.  For the egauge scraper, all
we need is a `gauges` section, which consists of a set of gauge
names, and their corresponding id numbers.

The simplest option for data-export is just to save our data,
as is, to a csv file.  With no other arguments given, this will
produce a timestamped file located in `tmp/outputs/projectname/`.

By default, all `acquire` steps produce rows of data with
the columns: `node`, `sensor`, `unit`, `timestamp`, and `value`.
We reshape our data by adding a `[reshape]` section to our configuration
file.  For specific information concerning data-reshaping, take a
look at [`doc/reshape-mappings.md`](./doc/reshape-mappings.md).


## Contribution

Pull requests welcome!
