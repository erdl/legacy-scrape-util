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
- *Node:* A central point that one or more sensors log to.
- *Sensor:* The named entity for which a given stream of
data has a one-to-one mapping.
- *Unit:* The units encoded by the sensor's readings.
- *Timestamp:* The time of the specific reading in question.
- *Value:* The reading its self.

The project will be the named instance over which a set of
configurations apply.  As such, a project can be a single persistent
variable across a given cycle of the application.  All remaining
fields are fields for which more than one value may exist, and as such
should be logged per instance of scraped data for clarity and simplicity.
Unit and Sensor will presumably change many times many times across
a single project instance, but presumably vary together.  Timestamp
and value should always be unique.

Next, we can break up the common phases of an application cycle into
a series of 'black-box' entities like so:

- *Data Acquisition:* The scrape portion of the cycle.
- *Data Reshaping:* The mapping portion of the cycle.
- *Data Logging:* The storage portion of the cycle.

...

# File-System Map

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
