# Reshape Mappings

The various implementations within the `reshape` step
can be a bit cryptic.  Below is a breakdown of how to
create configurations for the various mappings that are
possible during data reshaping.

## Overview

Currently, there are three separate kinds of reshape
operations:

- [`content`](#content-mapping) replace values of given fields with new values.

- [`field`](#field-mapping) modify naming, ordering, and/or existence of data fields.

- [`generated`](#generated-mapping) add additional generated fields (e.g.; upload timestamp).

Because of the nature of these tasks, the order of operations matters.
The `content` step will always occur first, with the `field` step always
preceding the `generated` step.  These ordering rules exist primarily in
the interest of the interoperability of configuration mappings.  We force
content mappings to conform to the `(node,sensor,unit,timestamp,value)`
schema such that they are interchangeable with any custom field mappings.
Likewise, we force generated fields to be appended to the final field layout
such that any field mappings can be completely agnostic of any generated fields
and vice-versa.

## Content Mapping

Content mappings are done on a per-field basis.  The top level of
the content configuration file/section should include a key for
each field that requires a content mapping.  Each field declaration
should contain a `map` section and an `ignore` section.  The map section
should be a key-value pairing of expected values to their desired replacements.
Lets say, for example, that we want to re-map our `sensor` field into a set
of integer ids.  We want to map `temperature` to `0`, `humidity` to `1`, and
we want to discard any data points from `lux` sensors.  We would achieve
this like so:

````javascript
"sensor" : {
  "map" : {
    "temperature" : 0,
    "humidity" : 1
  },
  "ignore" : [
    "lux"
  ]
}
````

Notice that we cannot simply leave `lux` out.  We don't want out script
to be silently ignoring unexpected values.  Any unexpected values are
instead saved to a csv file in `tmp/errors/projectname/` so that they can be
analyzed and re-integrated at a later date.

This is the simple content mapping case, and it works well for most use cases.
Sometimes, however, we find ourselves needing to create a mapping that is
dependent on the values of more than one field.  What if, for example, we
have two different ids for `humidity` based on whether the sensor
is associated with `logger_one` or `logger_two`?  As you may recall,
the `node` field is used in distinguishing named groupings of sensors
(building, logger, sub-project, etc...).  What we need, is to be able
to make two separate `sensor` mappings, depending on the content of the
`node` field.  We can achieve this with the `sub-map` flag.  This tells
the program that it is about to encounter a *nested* mapping; it will
need to split up the data based on the content some other field, and then
execute specific mappings on the target field.  This can be
a bit difficult to imagine at first, but lets take a look at what our
new `humidity` sensor mapping looks like:

````javascript
"sensor" : {
  "sub-map" : "node"
  "map" : {
    "logger_two" : {
      "map" : { "humidity" : 1 }
    }
    "logger_three" : {
      "map" : { "humidity" : 2 }
    }
  }
}
````

As we can see, we start with the same top-level field declaration;
telling the program that the final result of the mapping process
is targeted at the `sensor` field.  Inside, we declare a sub mapping
for the `node` field.  This tells the program to divide up the data
based upon the contents of the node field first.  We pair each possible
`node` value with the `sensor` mapping that applies to all data points
with that `node` value.  Ignore blocks are optional, but we could have easily
added an ignore block to the `node` mapping, or either of the
contained `sensor` mappings.

The `sub-map` flag is capable of being applied recursively.  If we so desired,
we could declare an additional sub mapping within one, or both, of our
existing `node` sub maps.  This feature, however, should be your *last*
resort.  If your data needs more than two levels deep of recursive
content remapping, you've done a terrible thing and you should be *ashamed*
of yourself.  Seriously.  What is *wrong* with you?

## Field Mapping

...


## Generated Mapping

...
