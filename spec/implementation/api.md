# Rust Library API

The `git-meta-lib` crate is the reference Rust library for storing, querying, serializing, and exchanging git-meta metadata from another Rust application. It is useful when you are building a tool that wants metadata to live beside a Git repository without adding files to the worktree.

Add it to your project and import the public API from the `git_meta_lib` crate name:

```toml
[dependencies]
git-meta-lib = "0.1.0"
```

```rust
use git_meta_lib::{MetaValue, Session, Target};
```

## The mental model

The public API is centered on three ideas:

- `Session` opens a Git repository and its local metadata database.
- `Target` identifies what metadata is attached to: a commit, path, branch, change ID, or the whole project.
- `MetaValue` stores one key as a string, list, or set.

Most applications create one `Session`, create `Target` values for the objects they care about, and use `session.target(&target)` to get a scoped handle for reading and writing keys on that target.

## Opening a repository

Use `Session::discover()` when your process is already inside a Git repository, or `Session::open(path)` when you have a repository path.

```rust
use git_meta_lib::Session;

let session = Session::discover()?;

println!("metadata namespace: {}", session.namespace());
println!("author email: {}", session.email());
```

The session opens or creates the local SQLite store at `.git/git-meta.sqlite`, reads Git config such as `user.email`, and uses the configured metadata namespace. The default namespace is `meta`, which produces refs such as `refs/meta/local/main` and `refs/meta/main`.

## Choosing targets

A `Target` describes the thing your metadata is about.

```rust
use git_meta_lib::Target;

let project = Target::project();
let commit = Target::commit("abc123")?;
let path = Target::path("src/main.rs");
let branch = Target::branch("main");
let change = Target::change_id("kqszpwnx");

let parsed = Target::parse("path:src/main.rs")?;
```

For commit targets, partial SHAs are accepted. If you need to normalize a commit target before storing metadata, call `session.resolve_target(&target)` to expand it through the repository.

## Reading and writing metadata

Call `session.target(&target)` to get a `SessionTargetHandle`. The handle automatically uses the session email and timestamp for mutations.

```rust
use git_meta_lib::{MetaValue, Session, Target};

let session = Session::discover()?;
let target = Target::path("src/main.rs");
let handle = session.target(&target);

handle.set("owner", "compiler-team")?;
handle.set("review:status", "approved")?;

if let Some(MetaValue::String(owner)) = handle.get_value("owner")? {
    println!("owner: {owner}");
}

for (key, value) in handle.get_all_values(Some("review"))? {
    println!("{key} = {value}");
}
```

Important handle methods:

- `get_value(key)` returns one typed value.
- `get_all_values(prefix)` returns all keys for a target, optionally filtered by a key prefix.
- `set(key, value)` writes a string, list, set, or explicit `MetaValue`.
- `remove(key)` tombstones one key.
- `get_authorship(key)` returns the last author email and timestamp for a key.

Keys use colon-separated namespaces such as `agent:model` or `review:status`. Prefix queries match the exact key or descendants under that namespace.

## Value types

git-meta supports three public value shapes.

### Strings

Strings are the simplest value type. Setting a string replaces the previous value.

```rust
handle.set("ci:state", "passed")?;
```

### Lists

Lists are ordered timestamped entries. They are useful for append-heavy data such as comments, events, transcript chunks, or logs.

```rust
handle.list_push("comments", "Looks good to me")?;
handle.list_push("comments", "Merged after CI passed")?;

for entry in handle.list_entries("comments")? {
    println!("{}: {}", entry.timestamp, entry.value);
}
```

You can also remove list items with `list_pop(key, value)` or `list_remove(key, index)`.

### Sets

Sets store unique unordered strings. They are useful for labels, reviewers, capabilities, and other membership-style metadata.

```rust
handle.set_add("labels", "security-reviewed")?;
handle.set_add("labels", "needs-docs")?;
handle.set_remove("labels", "needs-docs")?;
```

## Records over key prefixes

For small typed records, `set_record(prefix, record)` writes string fields to child keys under a prefix, and `get_record(prefix)` reads those child keys back through Serde.

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AgentInfo {
    name: String,
    model: String,
}

handle.set_record("agent", &AgentInfo {
    name: "codex".into(),
    model: "gpt-5".into(),
})?;

let agent: Option<AgentInfo> = handle.get_record("agent")?;
```

This writes keys such as `agent:name` and `agent:model`. Record writes are partial updates: fields serialized as `null` are skipped rather than deleted.

## Atomic edits

Use `apply_edits` when several changes should be committed to the local store as one transaction.

```rust
use git_meta_lib::{MetaEdit, MetaValue};

let status = MetaValue::String("approved".into());
let labels = vec!["ready".to_string(), "reviewed".to_string()];

handle.apply_edits([
    MetaEdit::set_value("review:status", &status),
    MetaEdit::set_add("labels", &labels),
])?;
```

If any edit fails, none of the batch is committed.

## Exchanging metadata through Git

Local reads and writes happen in SQLite. To exchange metadata with other clones, serialize the local store to Git objects and push or pull metadata refs.

```rust
let output = session.serialize()?;
println!("serialized {} changes", output.changes);

let push = session.push_once(None)?;
if push.non_fast_forward {
    session.resolve_push_conflict(None)?;
    let retry = session.push_once(None)?;
    assert!(retry.success);
}
```

Important workflow methods on `Session`:

- `serialize()` writes local metadata to the local metadata ref.
- `serialize_full()` rebuilds serialized state from the full SQLite store.
- `materialize(remote)` merges fetched metadata refs into the local store.
- `pull(remote)` fetches metadata from a configured metadata remote, materializes it, and indexes history for lazy loading.
- `push_once(remote)` serializes and attempts one push.
- `resolve_push_conflict(remote)` fetches and materializes remote changes after a non-fast-forward push rejection so the caller can retry.

The `remote` argument is `Option<&str>`. Passing `None` uses a primary configured metadata remote in preference to side remotes. Passing `Some("origin")` selects a specific metadata remote, including a side remote when explicitly named.

Long-running operations also have progress variants such as `serialize_with_progress`, `push_once_with_progress`, and `resolve_push_conflict_with_progress` for UI integration.

## What the library gives an application

If you are building a Rust system on top of git-meta, the main tools are:

- A fast local metadata database tied to a Git repository.
- Typed targets for commits, paths, branches, change IDs, and project-wide metadata.
- Namespaced metadata keys with string, list, and set semantics.
- Convenience APIs for typed Serde records over key prefixes.
- Atomic local edit batches.
- Authorship and timestamp tracking for mutations.
- Serialization and materialization between SQLite and the Git-tree exchange format.
- Pull, push, and push-conflict-resolution workflows built on normal Git refs and transport.
- Output structs that let a CLI, daemon, IDE integration, or code-review system decide how to present results without the library printing directly.

Most consumers should stay on the public API re-exported from `git_meta_lib`. The crate has an `internal` feature used by the reference CLI for lower-level plumbing, but ordinary integrations should not need it.
