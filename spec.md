# gmeta spec

(This is a human written document that was the starting point for a mostly AI generated example implementation. It lays out a lot of use cases I could think of and also some detailed examples of how I wanted it implemented.)

The `gmeta` tool is a Rust command line application, using Clap, that allows one to add structured metadata to Git data.

It stores this data in SQLite and provide CRUD operations via the command line to add, modify and delete key/value data for commits, change-ids, branch-ids, file paths or the project as a whole.

## SQLite

The SQLite file that contains this data is found or created at `.git/gmeta.sqlite`.

## Data

There will be two important data aspects to this. The first is the current value of any key. The second is a log of the mutation of the value of this key, so we can see what it was at any time. This log should have timestamps and an email address of the user who mutated it.

Each metadata value will have four parts. The target, the key, the value and the value type.

The target consists of two parts - the target type and the target string. This can be one of:

- 'commit', string is the Git commit SHA
- 'change-id', string is a UUID
- 'branch', string is a branch UUID or name
- 'path', string is a file path in the project
- 'project', no associated string - it's a global value

The key can be any arbitrary string. It can be namespaced with colons, for example 'agent:transcript' or 'agent:model:version'.

The value type can be one of three types (but we can extend this in the future).

- 'string' : single value
- 'list': array of strings

This allows us to have a set of simple operations for each value type (ie, 'append' for a list, 'insert' or 'replace' for a hash key)

The values will be mutated with the command line tool and will both update the log and have the most recent value quickly (O(1)) accessible in the design of the SQLite data system.

SQLite storage layout:

- `metadata` stores current key rows and string values
- `list_values` stores list items as one row per entry (`metadata_id`, `timestamp`, `value`)
- `metadata_log` stores mutation history
- `metadata_tombstones` stores latest removals

The command line tools will be:

`gmeta set [-t list] <target> <key> <value>` - if type is not given, assumes a string
`gmeta get <target> (<key>)` - show key value(s) (if partial key given or no key given, show all key/value pairs)
`gmeta rm <target> <key>` - remove key

Where `<target>` is `type:string`, for example, `commit:<SHA>`.

To set a list of values, `<value>` will be a JSON array and `-t list` must be specified. Otherwise, the value will be a JSON string rather than a list type.

For list values:

`gmeta list:push <target> <key> <value>` - adds to a list
`gmeta list:pop <target> <key> <value>` - removes from a list

If you push to a value that is a string, it will convert it to a list.

## Exchange

This data should be exchangeable over the Git protocol, meaning that we need to be able to serialize the current values into Git trees and commits and push them as references to any Git host.

We also need to be able to fetch these references from a Git host and materialize them locally into our own SQLite database.

We don't need the entire log to be serialized in the exchange format, only the most recent values.

The serialization format should be in Git tree format, so it can be easily diffed and transferred.

The commit pointing to the new serialized data should be stored under `refs/meta/local`, or something other than `meta` if `meta.namespace` Git config setting is set.

If you fetch from a remote with meta references, it should put that reference into `refs/meta/[remote-name]` (so, for example, `refs/meta/origin`).

### Serialize tree format

The base tree path for any target key should be the target type, then the first 2 char of the target value, then the last 3 char of the target value, then the full target value.

Keys under 3 char would not be valid.

Under this base path, keys are stored in a directory trie under `k/`. Key segments are split on `:` and written as raw directory names.

- String terminal blob name: `__value`
- List terminal directory name: `__list`
- Tombstone root directory name: `__tombstones`

This avoids Git blob/tree path collisions such as `agent` vs `agent:model`.

To keep the layout unambiguous, keys are strictly validated:

- key cannot be empty
- segments cannot be empty
- segments cannot be `.` or `..`
- segments cannot contain `/` or null bytes
- segments cannot equal reserved names `k`, `__value`, or `__list`

The `k/` root leaves room for future target-level metadata files without colliding with user key paths. Plausible future files/directories include:

- `__format` or `__schema` for target subtree versioning
- `__updated_at` for coarse target-level freshness metadata
- `__authorship` for compact target-level provenance
- `__merge_policy` for per-target merge strategy hints
- `__tombstones/` for explicit deletion markers
- `__signature` or `__digest` for integrity/provenance checks

### String values

For string values, write the string as a blob at `[base]/k/<key segments>/__value`.

So for example, if you run `gmeta set commit:13a7d29cde8f8557b54fd6474f547a56822180ae agent:model claude-4.6`, and serialize the data, you would get a Git tree:

```
❯ git ls-tree -r refs/meta/local
100644 blob a76e08d661b081b4e618e7e61066c879056c8f18 commit/13/0ae/13a7d29cde8f8557b54fd6474f547a56822180ae/k/agent/model/__value
```

If you `git cat-file -p a76e08d661b081b4e618e7e61066c879056c8f18` you would get the string `claude-4.6`.

The commit message of the serialization would have no commit message body, but would simply be used for the tree pointer and author/date information.

### List values

For list values, we store entries under `[base]/k/<key segments>/__list/` with `timestamp-hash` blob names so we can generally merge them while keeping order.

So, if we run: `gmeta set -t list branch:sc-branch-1-deadbeef agent:chat ["how's it going", "pretty good"]`

Our serialized tree will look like this:

```
❯ git ls-tree -r refs/meta/local
100644 blob b4e618e7e61066c879056c8f18a76e08d661b081 branch/sc/eef/sc-branch-1-deadbeef/k/agent/chat/__list/1771232450203-23c0f
100644 blob 066c879056c8f1b4e618e7e618a76e08d661b081 branch/sc/eef/sc-branch-1-deadbeef/k/agent/chat/__list/1771232450204-0d5f2
```

The `23c0f` are the first five chars of the SHA-256 hash of the message being stored, so messages at the same general time will almost certainly not collide from a merge of the same list of different users.

You can also notice that the second message is 1 millisecond from the first. We take the ms epoch timestamp of when the `set` command is run and if we're inserting multiple values, we increment the ms by 1 for each value so they're still in order.

### Removal tombstones

A remove operation records a tombstone in SQLite (`metadata_tombstones`) and serializes it into the Git tree.

Tombstone path format:

`[base]/__tombstones/k/<key segments>/__deleted`

The tombstone blob stores JSON with `timestamp` and `email`.

Example:

```
❯ git ls-tree -r refs/meta/local
100644 blob 6f31... commit/13/0ae/13a7d29cde8f8557b54fd6474f547a56822180ae/__tombstones/k/agent/model/__deleted
```

When a key is set again (`set`, `list:push`, `list:pop`), its tombstone is cleared in SQLite and no tombstone is emitted on the next serialize.

### Commands

The commands to serialize and materialize the data are:

`gmeta serialize` - write a new head to `refs/meta/local`
`gmeta materialize (<remote>)` - read from `refs/meta/remotes/*`, find anything not in local, make local sqlite data consistent. the `(remote)` is optional, otherwise we will look through all the heads under `refs/meta/remotes` and materialize all of them.

### Merging

When we `materialize` a remote meta head, we need to do four things.

1. merge that head into our `refs/meta/local`
2. resolve any merging conflicts
3. update our local sqlite database with all new values
4. record when we last materialized successfully

When we serialize again, we should look in our log for anything that has been modified since the last materialization, and only update the tree with those new values or mutations.

The `list` values should almost always cleanly merge. If there is an overlap of two tree entries with different shas, just drop one (it's almost impossible, since it's a timestamp _and_ partial content hash).

The `string` values can conflict if two users modified the same key. In this case, simply take the one with the later commit timestamp as the new value by default. In the future, we could add different merge strategies.

#### No Common Ancestor Merge

If `refs/<namespace>/local` and a remote metadata ref have no common ancestor (for example, two users initialized metadata histories independently), materialize uses a **two-way merge** instead of three-way merge.

Two-way merge policy:

1. Union keys from local and remote.
2. For overlapping keys (including value vs tombstone), **remote wins**.
3. Non-conflicting keys from both sides are kept.

`gmeta materialize --dry-run` reports this explicitly with `strategy=two-way-no-common-ancestor` and prints key-level conflict decisions.

#### Merging Removals

If you remove a key, materialize applies tombstones by deleting the key locally and recording the tombstone in SQLite.

If one side removed a key and the other modified it, choose the modified value (value wins over tombstone).

During fast-forward materialization, we also detect keys that existed in the previous local tree but are missing from the incoming value tree and remove them locally (legacy compatibility for trees that predate tombstones).

### Showing Values

There are several ways to show values. The `gmeta get` command can take only a target, a target and a key, or a target and a partial key. It can also take a `--json` argument to return the data in json format.

An example human output would be:

```
❯ gmeta get commit:13a7d29cde8f8557b54fd6474f547a56822180ae
agent:model  claude-4.6
agent:provider  anthropic
```

Or json:

```
❯ gmeta get --json commit:13a7d29cde8f8557b54fd6474f547a56822180ae
{
	'agent': {
		'model': 'claude-4.6',
		'provider': 'anthropic'
	}
}
```

With json, you can also add `--with-authorship` to add the commit timestamp and email address of when each entry was last modified and by whom.

```
❯ gmeta get --json --with-authorship commit:13a7d29cde8f8557b54fd6474f547a56822180ae
{
	'agent': {
		'model': {
			'value': 'claude-4.6',
			'author': 'schacon@gmail.com',
			'timestamp': 1771232450000
		}
		'provider': {
			'value': 'anthropic',
			'author': 'schacon@gmail.com',
			'timestamp': 1771232450000
		}
	}
}
```
