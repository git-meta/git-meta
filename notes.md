## Notes

- offload larger blobs to git references in sqlite (remove from sqlite file)
  - blob attachments as metadata
- what happens if two users on a project independently _start_ a meta ref? can you merge from two independent trees? rebase? what if they add the same key with different values?
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
