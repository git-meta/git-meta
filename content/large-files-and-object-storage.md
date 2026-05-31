---
title: dealing with large values
date: 2026-05-29
description: git-meta can keep values either in SQLite or in Git
author: schacon
---

Metadata starts out cute and tiny, right up until somebody decides that the value should be an entire agent transcript, a generated report, or some giant attestation blob.

That's when your nice little key/value store starts looking like a junk drawer.

Most `git-meta` values are boring in the best possible way. A model name. A review status. A URL. A short string that says something useful about a commit, branch, path, or project. Those values belong directly in SQLite because SQLite is fast, local, and extremely good at answering questions like "what is the current value for this target and key?"

But not everything should live inline forever.

## SQLite is the index, Git is the object store

The trick is that `git-meta` already has two useful storage systems available.

SQLite is the local working database. It tracks targets, keys, value types, authors, timestamps, and the current state of the world. That's the stuff you want to query quickly without walking Git history or inflating a bunch of objects for no reason.

Git, on the other hand, is very good at storing content-addressed blobs and moving them around with fetch and push. So when a value gets large enough, `git-meta` can write the actual payload as a Git blob and store the blob id in SQLite instead.

SQLite keeps the pointer. Git keeps the big thing.

## The cutoff

By default, the cutoff is 1 KiB. If a string value is larger than that, `git-meta` stores it as a Git blob reference instead of stuffing the whole thing into the SQLite row.

This applies whether the value came from the command line:

```bash
git meta set commit:HEAD agent:transcript "...some very large string..."
```

or from a file:

```bash
git meta set commit:HEAD agent:transcript -F transcript.jsonl
```

Originally, this only happened for `-F/--file` payloads. That was too cute. Large is large, regardless of whether it came from a file or from an inline argument, so the check now happens on the resulting value itself.

## You can change the size

The cutoff is not hardcoded policy anymore. You can set it with metadata too, which is pleasingly recursive.

There are two keys:

- `meta:sqlite:object-max-size` sets the shared project default.
- `meta:local:sqlite:object-max-size` sets your local override and wins if both are present.

Both accept plain byte counts or friendly sizes like `4k`, `64k`, or `1m`.

Set it low if you want SQLite to stay lean. Set it high if you want more values inline for easier inspection. Set it to `0` if you want every non-empty string value to become a Git blob reference.

Please do not set it to something ridiculous and then act surprised when the database becomes ridiculous. Computers are obedient, not wise.

## Reading it back

The blob reference is supposed to be an implementation detail.

When you run `git meta get`, you should get the value back, not a random-looking object id that makes you go spelunking through `.git/objects`. Internally, `git-meta` remembers whether a stored value is inline text or a Git reference, then resolves it on read when needed.

That distinction also matters during sync and materialization. Remote metadata can arrive with blob-backed values, and the local store needs to preserve that shape instead of accidentally turning an object id into the user-visible value.

## Why bother?

Because small metadata and large metadata want different tradeoffs.

Small values should be cheap, local, and queryable. Large values should not bloat the hot path just because we wanted to attach something useful to a commit. Splitting the two lets `git-meta` keep SQLite as the fast index and Git as the durable blob store.

That's the whole idea: use the boring parts for what they're good at.
