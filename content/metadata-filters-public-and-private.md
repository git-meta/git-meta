---
title: publishing metadata to the right places
date: 2026-05-31
description: serialize filters let git-meta keep local, company, and public metadata separate
author: schacon
---

One of the awkward things about metadata is that not all of it wants the same audience.

Some metadata is obviously public. Review status, ownership hints, generated documentation notes, compatibility information, provenance for a generated patch. If the repository is public, that metadata can probably travel with it.

Some metadata is useful only inside a company. Internal ticket IDs, security scan details, deployment approvals, customer names, private model evaluations, or links to systems that only exist behind the VPN.

And some metadata should not leave your laptop at all. Cursor positions, local scratch state, experiments, personal agent notes, temporary import markers.

If all of that lives in one pile, pushing metadata becomes scary. So `git-meta` has serialize filters.

## The default: publish normal keys

By default, metadata serializes to the main local metadata ref:

```text
refs/meta/local/main
```

That ref can then be pushed to a remote metadata ref, normally something like:

```text
refs/meta/main
```

So if you set ordinary keys:

```bash
git meta set project review:policy required
git meta set path:src/api.rs owner platform-team
git meta set commit:abc123 provenance:generator codegen-v2
```

then `git meta serialize` writes those keys to the normal serialized tree.

That is the happy path. If a key is not special and no filter matches it, it goes to `main`.

## Local-only keys

There is one hard rule that does not need configuration:

```text
meta:local:*
```

Anything under `meta:local:` is never serialized.

For example:

```bash
git meta set project meta:local:last-viewed-branch sc/experiment
git meta set path:src/api.rs meta:local:cursor-line 142
```

Those values stay in your local SQLite database. They are not written to any Git tree, even if you add a filter that tries to route them somewhere.

Use this namespace for personal state, caches, UI hints, and anything that would be noise or a leak if another clone saw it.

## Excluding keys

Sometimes a key is not purely local, but you still do not want it serialized. Add an `exclude` rule.

Filter rules are set members on the project target:

```bash
git meta set:add project meta:filter "exclude draft:**"
```

That means any key under `draft:` is skipped during serialization:

```bash
git meta set project draft:summary "not ready yet"
git meta set project draft:review-notes "too spicy"
```

The `**` wildcard means zero or more key segments, so `draft:title` and `draft:review:notes` both match.

A plain `*` matches exactly one segment:

```bash
git meta set:add project meta:filter "exclude scratch:*"
```

That matches `scratch:one`, but not `scratch:one:two`.

## Routing keys to another ref

The more interesting filter is `route`.

A route rule keeps matching keys out of `main` and writes them to another local destination ref:

```bash
git meta set:add project meta:filter "route company:** company"
```

Now this key:

```bash
git meta set commit:abc123 company:jira PROJ-1234
```

serializes to:

```text
refs/meta/local/company
```

instead of:

```text
refs/meta/local/main
```

That gives you two separate publishable streams:

- `main` for public metadata
- `company` for internal metadata

The serialized trees use the same exchange format; they just live under different refs.

## Shared filters vs personal filters

There are two places to put filter rules:

```text
meta:filter
meta:local:filter
```

Use `meta:filter` for project policy. These rules are themselves shared metadata, so everyone can agree that `company:**` goes to the company-only destination.

Use `meta:local:filter` for your own publishing preferences. Since it is under `meta:local:`, it is not serialized.

For example, a company might share this rule:

```bash
git meta set:add project meta:filter "route company:** company"
```

And you might locally add:

```bash
git meta set:add project meta:local:filter "route me:** mine"
```

Now company values go to the company destination, your personal values go to your private destination, and normal values still go to public `main`.

## A public plus company workflow

Imagine an open source project maintained by a company. The public repository should get useful metadata:

```bash
git meta set commit:abc123 review:status approved
git meta set commit:abc123 provenance:agent codex
git meta set path:src/billing.rs owner billing-team
```

But internal systems should stay internal:

```bash
git meta set commit:abc123 company:jira BILL-2841
git meta set commit:abc123 company:security-scan scan-99812
git meta set path:src/billing.rs company:service-id billing-prod
```

Set the shared filter once:

```bash
git meta set:add project meta:filter "route company:** company"
```

Then serialize:

```bash
git meta serialize
```

You now have at least two local metadata refs:

```text
refs/meta/local/main
refs/meta/local/company
```

The public ref can be pushed to the public host. The company ref can be pushed to an internal metadata remote or internal ref. The exact remote setup is host-tool policy, but the important bit is that the data is already separated before publishing.

Conceptually:

```bash
# public metadata
git push public refs/meta/local/main:refs/meta/main

# internal metadata
git push company refs/meta/local/company:refs/meta/main
```

Now public consumers can materialize review status, owners, and provenance without seeing internal Jira IDs. Company users can materialize the internal stream too.

## Multiple destinations

A route can name more than one destination with commas:

```bash
git meta set:add project meta:filter "route audit:** company,audit"
```

That writes matching keys to both destination refs. This is useful when one class of metadata should go to a broad internal audience and also to a narrower archive or compliance stream.

## Conflict rules are intentionally boring

Filters decide where a key is serialized. They do not change the meaning of the key, the target, or the merge rules for the value.

Strings are still strings. Lists are still append-friendly lists. Sets are still unique-member sets. A routed ref is just another serialized git-meta tree.

That is the design goal: keep policy about *where metadata goes* separate from semantics about *what metadata means*.

## A simple naming pattern

The practical advice is to pick obvious top-level namespaces:

```text
review:*        public review metadata
provenance:*    public generation/provenance metadata
owner           public ownership hint
company:*       company-only metadata
me:*            personal metadata routed by local preference
meta:local:*    never serialized at all
```

Then the filters are easy to understand:

```bash
git meta set:add project meta:filter "route company:** company"
git meta set:add project meta:local:filter "route me:** mine"
```

The result is boring in the good way: public metadata can be published publicly, company metadata can stay inside the company, and local metadata can remain local.

That is the point of serialize filters. Not secrecy magic, not access control, not encryption. Just a clean way to avoid mixing audiences before the data ever gets pushed.
