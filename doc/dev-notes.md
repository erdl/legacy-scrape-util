# Development Notes
Collection of notes about features & changes which are either
in progress, or are under consideration.  In essence; a feeble
attempt to push back against the screeching void that is the
need-driven development process.

## Configuration

*Format Independence*: Move to a format-independent model,
starting with full interoperability between `.json` and `.toml`
files.

*Config & State*: Move from the current model of `config` and
`nonce`, to the more general model of `config` and `state`.
Since different data-acquisition steps use the nonce differently,
or not at all, it makes more sense to give each data-acquisition step
access to a `state` file for storing & reading values.

## Cleanup

It may be useful to institute a framework by which 'cleanup' callbacks
can be registered by the data-acquisition step.  Such functions could
be run after the data-export step depending on its success or failure.
