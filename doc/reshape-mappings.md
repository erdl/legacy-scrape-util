# Reshape Mappings

The various implementations within the `reshape` step
can be a bit cryptic.  Below is a breakdown of how to
create configurations for the various mappings that are
possible during data reshaping.

## Overview

Currently, there are three separate kinds of reshape
operations:

- [`content`](#content-mapping) replaces values of given fields with new values.

- [`field`](#field-mapping) modifies naming, ordering, and/or existence of data fields.

- [`generated`](#generated-mapping) adds additional generated fields (e.g.; upload timestamp).

Because of the nature of these tasks, the order of operations matters.
The `content` step will always occur first, with the `field` step always
preceding the `generated` step.  These ordering rules exist primarily in
the interest of the interoperability of configuration mappings.  We force
content mappings to conform to the `(node,sensor,unit,timestamp,value)`
schema such that they are interchangeable with any custom field mappings.
Likewise, we force generated fields to be appended to the final field layout
such that any field mappings can be completely agnostic of any generated fields
and vice-versa.

## Value Mapping

Value mappings allow us to change one expected value into another for one or
more fields.  We instantiate a section for each field of interest, listing
expected values, and what they should be replaced with.  Lets imagine that
we have some set of data where the sensor corresponding to a given data point
may be `temperature` or `humidity`, but that we would prefer to identify our
sensors by numeric IDs instead.  Easy!  Just add a `[sensor]` section to
your value-mapping configuration file, and insert the mapping in the form
`oldValue = newValue`:

```toml
[sensor]
temperature = 0
humidity = 1
```

What if there is some data that we want to discard?  Just add an `ignores`
list to your configuration:

```toml
[sensor]
ignores = ["light"]
temperature = 0
humidity = 1
```

The ignores list is important if there are any expected values for which
you do not have an explicit mapping.  We don't want our conversions to fail
silently.  Any value which is not either mapped or ignored is something unexpected
and needs to be treated like an error.  Data points containing unexpected values
will be saved to a `csv` file in `tmp/errors/projectname/` for later examination.
If you happen to have `ignores` as an expected value, fear not!  You have two options;
you may precede `ignores` by any number of underscores (the field with the most underscores
will be treated as the ignores list), or you can segregate your settings and mappings like so:

```toml
[sensor.set]
...  # settings go here...
[sensor.map]
... # mappings go here...
```

What we have seen so far is the simple content mapping case, and it works well
for most use cases. Sometimes, however, we find ourselves needing to create a
mapping that is dependent on the values of more than one field.  What if,
for example, we have two different IDs for `humidity` based on whether the
sensor is associated with `logger-one` or `logger-two`?  As you may recall,
the `node` field is used in distinguishing named groupings of sensors
(building, logger, sub-project, etc...).  What we need, is to be able
to make two separate `sensor` mappings, depending on the content of the
`node` field.  We can achieve this with the `sub-map` flag.  This tells
the program that it is about to encounter a *nested* mapping; it will
need to split up the data based on the content some other field, and then
execute specific mappings on the target field.  This can be
a bit difficult to imagine at first, but lets take a look at what our
new `humidity` sensor mapping looks like:

```toml
[sensor]
sub-map = "node"

[sensor.logger-one]
humidity = 1

[sensor.logger-two]
humidity = 2
```

As we can see, we start with the same top-level field declaration;
telling the program that the final result of the mapping process
is targeted at the `sensor` field.  Inside, we declare a sub mapping
for the `node` field.  This tells the program to divide up the data
based upon the contents of the node field first.  We pair each possible
`node` value with the `sensor` mapping that applies to all data points
with that `node` value.  Ignore blocks are optional, but we could have easily
added an ignore block at any stage.  An ignore block next to `sub-map = "node"`
will let us ignore one or more possible node values, and an ignore block under
`[sensor.logger-one]` will allow us to ignore some set of the sensor values
which may appear alongside `logger-one`.  Here is what it would look like if
we wanted to ignore `logger-two`, and also ignore that pesky light sensor again:

```toml
[sensor]
sub-map = "node"
ignores = ["logger-two"]

[sensor.logger-one]
humidity = 1
ignores = ["light"]
```

The `sub-map` flag is capable of being applied recursively.  If we so desired,
we could declare an additional sub mapping within one, or both, of our
existing `node` sub maps.  This feature, however, should be your last
resort.  If your data needs more than two levels deep of recursive
content remapping, you have done a terrible thing and you should be *ashamed*
of yourself.  Seriously.  What is *wrong* with you?

## Field Mapping

...


## Generated Mapping

...
