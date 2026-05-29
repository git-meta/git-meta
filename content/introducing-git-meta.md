---
title: introducing git-meta
date: 2026-05-29
description: A short introduction to git-meta, an open specification for structured metadata in Git.
author: schacon
---

# introducing git-meta

The `git-meta` project is basically another stab at `git notes` - a way to attach arbitrary metadata to things in Git without needing to rewrite information.

If you're familiar with `git notes`, you know that it allows you to attach a single blob of data to a single Git object (normally a commit) and use Git push/fetch commands to move them around. They are somewhat rarely used because of a number of shortcomings such as:

- complexity around merging from two contributors
- the limitation of one value per commit
- the inability to attach data to other things like paths or the project as a whole
- scalability issues
- and more!

The `git-meta` project was started to address this. With `git-meta`, you can:

- attach values to various types of targets in the project (branches, paths, commits, change-ids)
- have namespaced key/value pairs (ie `agent:model` on a commit)
- have rich value types (strings, sets or lists)
- merge multi-user metadata easily
- use multiple sharing targets
- scale to many millions of keys easily
- and more!

We created this specification, a Rust library, and a reference CLI implementation to allow everyone to easily attach and manage arbitrary metadata to various parts of their existing Git codebases with minimal difficulty.

## Simple example

Here's how it works. You can install it with Cargo:

```
cargo install git-meta-cli
```

Then you have the `git-meta` CLI tool that can manage everything. Attach a new arbitrary value on a commit with the `git meta set` command:

```
❯ git meta set commit:314e7f0fa7 agent:model "claude-opus-4-8[1m]"
```

That will look up the commit, expand it to the full SHA and place a value under the key `agent:model` attached to that commit. You could also set `agent:provider` or any other key/value combination.

Then you can get the value back out with `git meta get`:

```
❯ git meta get commit:314e7f0fa7 agent:model
agent:model  claude-opus-4-6[1m]
```

You probably get the idea. You can also [assign sets or lists of values](https://github.com/git-meta/git-meta#value-types) to a key if that makes more sense for the data type.

## Sharing

You can easily setup a meta remote (someplace to push the data to) with `git meta setup` which will default to the same repository as your code, but under a hidden `refs/meta/main` reference. So it can, for example, be on GitHub with your code but just not visible.

Then just type `git meta sync` to push new values you've created and pull down new values from other users on your team.

## Try it out

The project is still early, but the core direction is in place: make Git metadata portable, local-first, and tool-friendly. Try it out and give us feedback!
