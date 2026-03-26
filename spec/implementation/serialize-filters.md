# Serialize filters

This document describes how metadata keys can be excluded from serialization or routed to alternative refs during the serialize step.

## Problem

Some metadata is only meaningful locally and should never leave the machine. Other metadata may be useful to share, but only within a limited scope (e.g. a personal ref rather than the shared team ref).

Today, all keys in the database are serialized into `refs/meta/local` unconditionally. There is no way to keep a key local-only or to direct certain keys to a different ref.

## Design

### Local-only keys via the `meta:local:` namespace

Any key whose first segments are `meta:local` is **never serialized** to any ref. They are only available in the local storage.

Examples:

- `meta:local:scratch`
- `meta:local:editor:cursor`
- `meta:local:build:last-status`
- `meta:local:ai:draft-summary`

The `meta:local:` prefix is a hard rule enforced by the serializer. No filter configuration is needed to make it work. Keys in this namespace are silently skipped during serialize and are never written into any git tree.

On materialize, `local:` keys in an incoming tree are ignored (they should not exist, but if they do, they are skipped).

### Filter rules via `meta:local:filter`

Users can define filter rules that control serialization behavior. Filter rules are stored as set members on the **project** target under the key `meta:local:filter`.

Each set member is a rule string with the format:

```
<action> <pattern> [<destination>]
```

#### Actions

| Action    | Meaning                                                                      |
| --------- | ---------------------------------------------------------------------------- |
| `exclude` | Never serialize matching keys to any ref. They remain local-only.            |
| `route`   | Serialize matching keys to a named secondary ref instead of the default local ref. Requires a `<destination>` argument. |

#### Patterns

Patterns match against the full key string using a simple glob-like syntax:

| Syntax  | Meaning                                                                                 |
| ------- | --------------------------------------------------------------------------------------- |
| `*`     | Matches any sequence of characters within a single key segment (between `:` delimiters) |
| `**`    | Matches any number of complete key segments (including zero)                            |
| literal | Exact segment match                                                                     |

Segments are delimited by `:`.

Examples:

| Pattern          | Matches                                            | Does not match                     |
| ---------------- | -------------------------------------------------- | ---------------------------------- |
| `draft:*`        | `draft:summary`, `draft:notes`                     | `draft:ai:summary`                 |
| `draft:**`       | `draft:summary`, `draft:ai:summary`, `draft:x:y:z` | `notes:draft:x`                    |
| `agent:*:prompt` | `agent:claude:prompt`                              | `agent:prompt`, `agent:x:y:prompt` |
| `myteam:**`      | `myteam:anything:at:any:depth`                     | `yourteam:x`                       |
| `wip`            | `wip`                                              | `wip:notes`                        |

#### Route destination

The `route` action takes a third argument specifying the destination name. The destination becomes a sub-ref under the local ref:

```
refs/meta/local/<destination>
```

For example, `route myteam:** private` serializes matching keys to `refs/meta/local/private`.

Each destination ref is a separate commit/tree that contains only the keys routed to it, using the same tree structure as the primary ref. Each can be pushed independently (e.g. to a personal remote or a different refspec).

If a `route` rule matches, the key is **excluded** from the primary `refs/meta/local` tree and **included** only in its destination ref.

If the namespace config is set (e.g. `meta.namespace = foo`), a destination `private` becomes `refs/foo/local/private`.

Multiple route rules can target different destinations. Keys from all rules sharing the same destination are collected into a single ref.

### Rule evaluation

Rules are evaluated in order of specificity:

1. The `meta:local:*` hard rule always wins. `meta:local:` keys are never serialized regardless of any filter rules.
2. Filter rules from `meta:local:filter` are evaluated. If multiple rules match the same key, the **first matching rule** (by set member sort order) applies.
3. Keys that match no rule are serialized to the primary ref as usual.

### The filter set itself

The key `meta:local:filter` starts with `meta:local:`, so it is **never serialized**. Filter rules are always local-only. This means each collaborator maintains their own filter rules independently.

## Serialize algorithm changes

The current serialize flow is:

1. Read all metadata from SQLite (`get_all_metadata`)
2. Read all tombstones
3. Build git tree entries for each key
4. Commit to `refs/meta/local`

The new flow becomes:

1. Read all metadata from SQLite
2. Read all tombstones
3. Read filter rules from `meta:local:filter` on the project target
4. For each key:
   - If key starts with `meta:local:` -> skip entirely
   - If key matches an `exclude` rule -> skip entirely
   - If key matches a `route` rule -> add to the tree builder for that rule's destination
   - Otherwise -> add to the primary tree builder
5. Apply the same skip/route logic to tombstones
6. Commit the primary tree to `refs/meta/local`
7. For each destination that has entries, commit its tree to `refs/meta/local/<destination>`

### Incremental serialization

The dirty-target tracking for incremental serialization applies to the primary tree and all destination trees. A change to `meta:local:filter` marks the project target dirty, which triggers a full re-evaluation of project-target keys.

If filter rules change such that keys move between refs, the next full serialize will correct all trees. Incremental serialize will handle this correctly as long as the project target is marked dirty when `meta:local:filter` changes (which it will be, since it is itself a key on the project target).

## CLI surface

No new commands are needed. Filter rules are managed with the existing `set` commands:

```sh
# Add a filter rule
gmeta set -t project meta:local:filter --set-add "exclude draft:**"
gmeta set -t project meta:local:filter --set-add "route myteam:** private"
gmeta set -t project meta:local:filter --set-add "route acme:** vendor"

# View current filter rules
gmeta get -t project meta:local:filter

# Remove a filter rule
gmeta set -t project meta:local:filter --set-rm "exclude draft:**"
```

Local-only keys are set and read like any other key:

```sh
gmeta set -t commit:abc123 meta:local:scratch "my working notes"
gmeta get -t commit:abc123 meta:local:scratch
```

## Examples

### Keep draft notes local

```sh
gmeta set -t project meta:local:filter --set-add "exclude draft:**"
gmeta set -t commit:abc123 draft:summary "WIP: still thinking about this"
gmeta serialize   # draft:summary is not in the git tree
```

### Route personal annotations to a separate ref

```sh
gmeta set -t project meta:local:filter --set-add "route myname:** mine"
gmeta set -t commit:abc123 myname:review-note "looks good but check error handling"
gmeta serialize   # review-note goes to refs/meta/local/mine, not refs/meta/local
```

### Route different namespaces to different refs

```sh
gmeta set -t project meta:local:filter --set-add "route myname:** mine"
gmeta set -t project meta:local:filter --set-add "route acme:** vendor"
gmeta serialize   # myname:* keys go to refs/meta/local/mine
                  # acme:* keys go to refs/meta/local/vendor
```

### Always-local scratch space

```sh
gmeta set -t commit:abc123 meta:local:cursor-pos "line 42"
gmeta serialize   # meta:local:cursor-pos is never serialized, no filter needed
```

## Non-goals

- Per-target filtering (all filters apply globally across targets)
- Regex patterns (glob syntax is sufficient)
- Filter rules on non-project targets
- Nested destination refs (destination is a single name, not a path)
