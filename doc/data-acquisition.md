# Data Acquisition

The *Data Acquisition* step, like the rest of the collection,
needs to adhere to a set of input and output standards that
allow it to be effectively interchangeable with any other
*Data Reshaping* and *Data Export* implementations.

## Input

All implementations need to have a single entry-point with
a standardized set of arguments.  The current standard is to implement
a function named `scrape` which accepts the arguments `project`,
`config`, and `nonce` (in that order).

The `project` argument is a string representing the project name.
Typically, this value is only used if errors occur during
data acquisition, but this may depend on the internals of a given
implementation.

The `config` argument is a dictionary of configuration values specific
to the given implementation.  The only field that the configuration
is guaranteed to contain is a `type` field, which is how the main
program determines which data acquisition module to launch.

The `nonce` argument is a dictionary which *may* contain one or more
key-value pairs which inform the data acquisition module where
it left off last time it ran a scrape.  If a given implementation
uses the nonce value, it is standard practice to have it default to
scrape the past 24 hours for any elements which are not in the nonce.
Because different implementations may use the nonce values in different
ways (or not at all), a given implementation is responsible for
returning an updated version of the nonce if such functionality is
desired.  Implementations which do not use the nonce should
return an empty dictionary instead.

## Output

In order for the *Data Acquisition* and *Data Reshaping* steps to be
truly interchangeable, it is necessary for a standardized data structure
to be passed between them.  The current standard is to return a list of `Row`
objects.  The `Row` object is a special
[`namedtuple`](https://docs.python.org/3/library/collections.html#collections.namedtuple)
constructor with five fields: `node`, `sensor`, `unit`, `timestamp`, and `value`.
**Ex:**

````python
In [1]: from src.core.row import Row

In [2]: Row('room203', 'light', 'lux', 946166399, 55)
Out[2]: row(node='room203', sensor='light', unit='lux', timestamp=946166399, value=55)

````

Generally speaking, the five fields of the row object should conform to
this model:

- *Node:* A named entity that one or more sensors relate to
(logger, building, etc...).
- *Sensor:* The named entity for which a given stream of
data has a one-to-one mapping.
- *Unit:* The units encoded by the sensor's readings.
- *Timestamp:* The time of the specific reading in question.
- *Value:* The reading itself.

The `node`, `sensor`, and `unit` fields should all be strings,
while the `timestamp` and `value` fields should both be floats.
This is not strictly enforced, but a number of reformatting
modules do assume this, and as such it should be considered
the standard for this model.  If `node` or `unit` are not
available, they can be given a default value of `"undefined"`,
but if `sensor`, `timestamp`, or `value` are unavailable, this
should be treated as an error.

## Boilerplate

Basically, just copy-pasta this template into your implementation,
and then get to working making it do what you need:

````python

from src.core.row import Row

def scrape(project,config,nonce):
    # do all the things!
    # ...
    return rows,nonce

````

## Contribution

If you have made an acquisition module that plays nicely with
the rest of the ecosystem, please feel free to submit it!
