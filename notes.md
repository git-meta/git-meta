## Notes

- remove k/ and just use \_\_ prefix for non-key values
- serialize and materialize debugging output
- delete specific list entries (by value?)
- partial path reads (gmeta get path:src owner)
- namespaces (local, shared, internal, etc - push targets (none, remote)
  - materialize targets too
  - on conflicts, which wins?

## Scenarios

- simple
  - user A adds a key, serializes, pushes to meta remote
  - user B fetches, materializes, adds a key, modifies the first key, pushes to remote
  - user A adds a third key
  - user A fetches and materializes, has all 3 keys

## Stuff Butler needs to do

- transfer metadata
