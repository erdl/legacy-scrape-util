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
fields are fields for which more than one value may exist within a single,
set of data, and as such should be logged on a
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

## File-System Map

````
tmp
.
|___projects
    .
    |___projname
        .
        |___config.json
        |___...
|___outputs
    .
    |___projname
        .
        |___...
|___errors
    .
    |___log.txt
    |___...

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

For specific information concerning the configuration of
mappings for the reshaping step, see the description and examples
in [`doc/reshape-mappings.md`](./doc/reshape-mappings.md).


## Contribution

Pull requests welcome.
