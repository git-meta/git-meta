# Materialization

This document describes the rules for turning a serialized metadata tree back into local current state.

## Materialize responsibilities

When materializing a remote metadata head, the client must:

1. merge that head in memory into local serialized metadata
2. resolve merge conflicts according to value-type semantics
3. update the local SQLite database with newly visible values and tombstones
4. record when materialization succeeded

Per-type conflict rules are defined in:

- [Strings](./strings.md)
- [Lists](./lists.md)
- [Sets](./sets.md)

## High-level scenarios

### 1. Initial sync

If the local system has no metadata yet:

- walk the tree at the incoming metadata head
- materialize all visible values into SQLite
- record that metadata commit as materialized

No metadata history walk is required beyond the current tree.

### 2. Multiple start points

If local metadata exists and someone else already pushed an independently initialized metadata history:

- serialize local shareable state
- perform a baseless two-way merge with the remote tree
- for overlapping keys, remote wins
- write a new metadata commit with the remote head as parent
- retry if the remote advanced before push completed

This allows eventual convergence while keeping history linear.

### 3. Fast-forward update

If the incoming metadata ref is a fast-forward from the last materialized point:

- diff the old materialized tree against the new tree
- apply only changed key/values to SQLite
- add or update visible values and tombstones

### 4. Both sides mutated data

If local data changed and remote data also changed:

- serialize local current shareable state
- merge the remote tree into it
- resolve conflicts according to per-type semantics
- write a new metadata commit from the merged tree

On push, if it is again not a fast-forward, do this sequence repeatedly against newer remote heads until a fast-forward push succeeds. Only one new commit locally needs to be made.

## No common ancestor merge

If local and remote metadata histories have no common ancestor, materialize uses a two-way merge instead of a three-way merge.

1. union non-conflicting keys from both sides
2. for overlapping keys or value-vs-tombstone conflicts, resolve according to
   the value-type merge and removal rules (for example, lists and sets can
   union while scalar conflicts choose one side)
3. retain non-overlapping keys from both sides

## Scoped materialization

Some implementations materialize a single metadata ref that intentionally
contains only a subset of keys. In that case, the same materialization rules
apply, but only to keys and tombstones that belong to that scope.

Keys outside the scoped ref are not considered deleted just because they are
absent. Deletion is still represented only by explicit tombstones in the scoped
tree.

## Removal handling

Deletion is explicit.

During materialization:

- whole-key tombstones remove keys locally
- per-entry or per-member tombstones are applied according to the collection type

## Metadata history shape

Unlike source-code history, metadata history does not need rich branch/merge topology.

The preferred shape is linear history created by repeatedly:

- merging in memory
- writing a new commit
- fast-forward pushing

The main goal is convergence on current metadata state, not preserving meaningful branch structure.

## Merging

- update locally, pruned remotely?
