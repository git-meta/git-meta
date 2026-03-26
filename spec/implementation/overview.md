# Implementation Overview

Everything in this "Implementation" section is how the reference implementation of this metadata spec operates. It is not neccesary for any other implementation to do the same things, but I figured it would be nice to document how I decided to make this work as a plumbing set that follows the exchange spec.

I have decided to keep the local data cache in SQLite, which is documented in [Local storage](./storage.md).

I have implemented the basics in Rust as a CLI that can get and set values, serialize and materialize. This CLI surface is documented [here](./cli.md).

That CLI has a few ways of returning data, which I document in [Output and query semantics](./output.md).
