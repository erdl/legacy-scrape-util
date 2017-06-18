# Data Acquisition

This document contains information concerning the usage of
the pre-existing data-acquisition modules in this utility.
It is worth reading the top-level README file before reading this
document, as it contains an overview of the config file format
which is used in this project.

## Egauge

The `egauge` acquisition module is used for acquiring data via the
[eGauge](https://www.egauge.net/) web api.

### `[gauges]`

The `[gauges]` section is the only required configuration section for
the `egauge` module.  It should consist of a set of key-value pairs
corresponding of some arbitrary name and the target gauge number
respectively. *Ex:*

```toml
[gauges]
gauge-1 = 123
gauge-2 = 345
```

The guage name specified will appear as the value of the `node` field
in the data-points resulting from that gauge.  The `name`, `unit`,
`timestamp`, and `value` fields are all inferred from the egauge data.

### `[[filter]]`

Due to the nature of the egauge API, queries return all data-points
available within the specified time range.  Since it can be tediuous
to filter out large amounts of data during the `reshape` step, a limited
subset of filtering functionality is available directly within the `egauge`
module.  Take a look at this example:

```toml
[[filter]]
mode = "positive"
name = "usage"
``` 

The above filter matches on all data-points, for which the value of the `name`
field is 'usage' (all text matching is case-insensitive).  Because we have
specified that the filtering mode is positive, only matching data-points are
retained.  Conversely, if we specified the filtering mode to be negative,
only non-matching values would be retained.

Double bracketed section headers (ex: `[[foo]]`), are 'array' style
headers.  Any number of them may be declared, and they will be executed in
order.  If, for example, one wanted to save all data-points with the unit `kW`,
except for `outdoor-lights`, the effect could be achieved like so:

```toml
[[filter]]
mode = "positive"
unit = "kW"

[[filter]]
mode = "negative"
name = "outdoor-lights"
```

Additionally, match strings may start and/or end with the the wildcard character 
(`'*'`) in order to match more than one possible permutations.  *Ex:* `"foo*"` matches
any value that *begins* with `foo`, while `"*foo"` matches any value that *ends* with `foo`,
and `"*foo*"` matches any value which contains `foo`.


## Webctrl

TODO


## Static

The `static` data-acquisition module is slightly unique in that it actually contains
a series of sub modules (referred to as parsers), which can be individually called.
The `static` module provides a set of common features and behaviors when the 
source of data-acquisition is a static file which already exists on the local machine.

### `[settings]`

The `[settings]` section is optional, and contains a set of configuration values
which describe the default directories for various file actions.  Lets take a look
at the settings section from a project that is parsing `hobo-u12` data as part of
an energy usage audit:

```toml
[settings]
source = "tmp/inputs/energy-audit-u12/"
on-fmt = "tmp/archive/energy-audit-u12/"
on-err = "tmp/errors/energy-audit-u12/"
on-raw = false
```


 
