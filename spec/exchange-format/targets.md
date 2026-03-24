# Targets and keys

This document defines what metadata can be attached to and how keys are structured.

At it's core, every piece of metadata is a tuple of `(target, key, value)`. The target is something like a single commit, changeset or path. This is something you can attach metadata to. Then for each target there is series of key/value pairs scoped to that target.

In this specification, the key can be namespaced (such as `source:issue:github:id`) where a query may be interested at data under level of the key namespace.

The value can furthermore be of various types of simple data primatives, such as a simple string, a list or an unordered set.

## Target model

Every metadata value is scoped to a target.

A target has two conceptual parts:

- `target_type`
- `target_value`

Supported target types:

- `commit` — target value is a Git commit SHA
- `change-id` — target value is a UUID
- `branch` — target value is a branch UUID or name
- `path` — target value is a file or directory path in the project
- `project` — global project scope; no associated target value in the user-facing model

The spec is written in such a way that new target types should be relatively easy to add in future spec versions.

## Keys

Keys are arbitrary strings with optional namespace structure.

Examples:

- `owner`
- `agent:model`
- `agent:provider`
- `agent:claude:session-id`

Keys are split on `:` into path segments during serialization and so cannot contain a ':' in the key segment itself.

## Key validation

To keep the exchange tree layout unambiguous, keys are strictly validated.

Rules:

- key cannot be empty
- key segments cannot be empty
- key segments cannot be `.` or `..`
- key segments cannot contain `/` or null bytes
- key segments cannot start with `__`

The last rule reserves all `__*` path components for gmeta structural metadata. If they are needed (for example, file paths), they can be escaped.

## Key path reservation

Keys are serialized directly under the target base path.

Any path component beginning with `__` is reserved for gmeta structural paths such as:

- `__value`
- `__list`
- `__set`
- `__tombstones`
- `__target__` (used only as the separator inside serialized `path` targets)

This means user keys occupy normal path segments and metadata structure begins when a reserved `__*` path component is encountered.

## Path target encoding

`path` targets are serialized using their raw path segments rather than a hash-derived fanout prefix.

Shape:

`path/<escaped path segments...>/__target__/...`

Rules:

- each `/`-separated path component becomes one tree path component
- `__target__` marks the end of the target path and the beginning of key segments
- if a path segment begins with `__`, escape it by prefixing the segment with `~`
- if a path segment begins with `~`, also escape it by prefixing the segment with `~`

Examples:

- `path:src/metrics` → `path/src/metrics/__target__/...`
- `path:src/__generated/file.rs` → `path/src/~__generated/file.rs/__target__/...`
- `path:src/~scratch/file.rs` → `path/src/~~scratch/file.rs/__target__/...`

## Value types

Current and proposed value types:

- `string` — single scalar string value
- `list` — append-friendly ordered sequence of string entries
- `set` — unordered unique collection of string members

The per-type exchange semantics are defined in:

- [Strings](./strings.md)
- [Lists](./lists.md)
- [Sets](./sets.md)
