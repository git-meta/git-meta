# Remote Management

Some implementation examples of how implementations should manage metadata remotes and coordinate serialization/materialization.

## Adding a remote meta source

The workflow starts by adding a remote source. This could be automatically done by a host tool if it sees project setup, such as a local `.git-meta` file.

```bash
$ git meta remote add (url)
```

The implementation should inspect the server for `refs/<namespace>/main`, where the default namespace is `meta`, and set up a normal Git remote that is marked as metadata-aware in `.git/config`.

For the first metadata remote, the local tracking ref is the primary remote-tracking metadata ref:

```ini
[remote "meta"]
        url = git@github.com:schacon/entire-meta.git
        fetch = +refs/meta/main:refs/meta/remotes/main
        meta = true
        serialize = main[:refs/meta/main]
        promisor = true
        partialclonefilter = blob:none
```

The `remote.<name>.meta = true` boolean identifies this Git remote as a metadata remote. The URL may be the same as an existing code remote, but implementations may keep metadata remotes separate so they can use independent fetch refspecs, partial clone settings, and permissions.

The `serialize = [name]` entry tells the implementation where serialized values for a destination should be pushed. If serialization filters write to different destinations, such as `internal` or `mine`, each serialized local ref head should have a remote entry with a matching value. This is also how the implementation constructs push refspecs.

After checking the source and setting up the Git remote, the implementation should do an initial blobless fetch of the metadata ref:

```bash
git fetch --filter=blob:none meta refs/meta/main:refs/meta/remotes/main
```

Next we need to do the equivalent of a `git checkout` on that head, so Git will do the promisor remote "want" conversation to get everything in the tip tree.

The best way to do this that I can find is to get all the blobs with `ls-tree` and pipe the list into `fetch` (with some complicated options) which seems to do what we want. The OID list must be sent in bounded batches, not as one giant request, because large metadata trees can otherwise exceed smart-HTTP request limits.

```bash
git ls-tree -r --object-only meta/remotes/main > tip-oids

# For each bounded chunk from tip-oids:
git -c fetch.negotiationAlgorithm=noop fetch meta --no-tags --no-write-fetch-head --recurse-submodules=no --filter=blob:none --stdin < tip-oids.batch
```

Now we have the tip tree data and can do some fast metadata lookups for recent stuff. If we need to get other blobs, we can do the same basic trick - figure out the list of blobs you need from the commit tree history, run them through `fetch` to get a packfile of them.

It doesn't have to be a top level tree, we could look up any set of blob values we want and send them to `fetch` this way. Imagine wanting all the metadata for a range of commits - we walk the history past prune metadata commits to the last times we've seen any of these SHAs and walk the subtrees to see what all blobs are referenced in any of them and then _just_ ask for those dozens of content blobs.

## Multiple metadata remotes and side refs

All fetched metadata refs should live under the normal `refs/<namespace>/remotes/...` area. Implementations should not use a separate `refs/<namespace>/remote/...` namespace to distinguish side refs from primary refs.

A metadata remote is a **primary** remote unless its Git config section has a side-ref flag:

```ini
[remote "history"]
        url = git@github.com:schacon/history-meta.git
        fetch = +refs/meta/main:refs/meta/remotes/history/main
        meta = true
        metaside = true
        promisor = true
        partialclonefilter = blob:none
```

The `remote.<name>.metaside = true` flag means the remote is readable/materializable but is not a default publication target. Its fetched ref is stored at:

```text
refs/<namespace>/remotes/<name>/main
```

For example:

```bash
git fetch --filter=blob:none history refs/meta/main:refs/meta/remotes/history/main
```

When `git meta remote add` is run and a primary metadata remote already exists, the newly added metadata remote should automatically be configured as a side ref by setting `remote.<name>.metaside = true` and by using a remote-specific tracking ref such as `refs/meta/remotes/<name>/main`.

When resolving a metadata remote implicitly, implementations should prefer primary remotes. A side remote should still be accepted when explicitly named, for example `git meta pull history`.

Materialization should treat configured side refs as additional readable metadata sources. Values imported from side refs should be marked as originating from that source and should not be reserialized into `refs/<namespace>/local/main` unless they are edited locally. A local edit clears the side-ref origin, making the value publishable according to normal serialization rules.

## Pushing and Pulling

Eventually we'll need to incorporate some automatic version of this into GitButler itself, but as a mid-level plumbing solution, we can do a `git meta push` and `git meta pull` that could be called by something else, such as Git hooks.

### Pushing

`git meta push` relies on the `fetch` and `serialize` config values on a primary `meta` tagged remote. If more than one metadata remote exists, implicit push should prefer a primary remote and should not push to side remotes unless explicitly configured to do so.

The simplest outcome of a `git meta push` is to serialize a new tree and commit on the metadata history and push it upstream as a fast-forward.

The more complex case is that there is data we have not seen yet upstream, so we need to pull that down, serialize our own tree, merge the trees, materialize the outcome, then write a new tree and commit on top and try to push again. If we weren't fast enough and there is new data upstream again, we repeat. It should _always_ result in a single new commit written locally, even if we had to try several times.

### Pulling

A `git meta pull` should fetch the selected remote's `refs/<namespace>/main` into that remote's configured local tracking ref, serialize our side if we have new data, merge the trees, and materialize the new tree locally.

For a primary remote, the default tracking ref is:

```text
refs/<namespace>/remotes/main
```

For a side remote named `history`, the tracking ref is:

```text
refs/<namespace>/remotes/history/main
```

### Serializing for Push

We may want to keep our last serialized commit locally, but if we go to write another one and the last one was ours and unpushed, rewrite it. Always keep and push the minimum number of new commits necessary.

### Serialize Commit Messages

Serialization commits encode a diffstat of the changes they introduce. This allows fast key-list compilation from commit history without fetching any blob data — critical for blobless clones where the tree and commit objects are cheap but blob fetches are expensive. Walking the commit messages to reconstruct which keys exist is orders of magnitude faster than materializing trees.

There are two commit message formats:

**Normal** (up to 1000 changes):

```text
git-meta: serialize (3 changes)

A	commit:abc123...	agent:model
M	commit:abc123...	agent:cost
D	project	meta:old-key
```

Each change line is: `A`/`M`/`D` (add/modify/delete), a tab, the target, a tab, the key.

**Large** (over 1000 changes):

```text
git-meta: serialize (5432 changes)

changes-omitted: true
count: 5432
```

When the change count exceeds 1000, the individual lines are omitted to keep commit objects small. Consumers should fall back to tree diffing for these commits.

## Removing a remote meta source

A user may want to get rid of a meta source they are no longer using.

```bash
git meta remote remove [name]
```

It should remove the `.git/config` entry for that remote, including `remote.<name>.meta`, `remote.<name>.metaside`, promisor settings, and fetch refspecs. It should also remove local metadata refs owned by that remote, such as `refs/<namespace>/remotes/main` for a primary remote or `refs/<namespace>/remotes/<name>/main` for a side remote. Removing a remote should not delete unrelated `refs/<namespace>/local/*` refs that belong to local serialization destinations.
