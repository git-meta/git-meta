# Standard keys

This document suggests a small set of commonly useful metadata keys.

These are not mandatory exchange-format rules. They are implementation-level recommendations intended to improve interoperability between tools that use git-meta.

> [!NOTE]
> To propose a new standard key, change to an existing one, or report an issue with this list, [open a GitHub issue](https://github.com/git-meta/git-meta/issues/new?labels=standard-keys&title=Standard+keys%3A+).

## Metadata Keys

### Agents

Agent-generated metadata is likely to be common, so a stable namespace is useful.

These keys can be attached to `commit`, `change-id` or `branch` targets.

Implementations should avoid storing secrets in agent metadata keys.

```key agent:provider
type: string
meaning: service or runtime provider that produced the content
examples:
  - openai
  - anthropic
  - local
```

```key agent:model
type: string
meaning: model identifier used for generation or analysis
examples:
  - gpt-5
  - claude-sonnet-4
  - llama-3.3-70b
```

```key agent:session-id
type: string
meaning: provider or tool session identifier
format: opaque stable string from the originating system
```

```key agent:prompt
type: string
meaning: canonical prompt or final instruction associated with the target
```

```key agent:summary
type: string
meaning: human-readable summary of what the agent did or concluded
```

```key agent:transcript
type: list
meaning: ordered record of the agent session, one message per list item
format: each item is a single JSON Lines (JSONL) record encoding one message (role, content, and any tool calls)
```

## Format Recommendations

### Naming

Keys should use a stable namespace-like structure with `:` separators.

Recommended conventions:

- use short, lowercase segments
- use a broad domain first, then more specific segments
- prefer singular nouns for scalar values
- reserve plural concepts for collection-typed keys
- keep the same key meaning across all target types when possible

### Format

Where possible, string values should use simple portable text formats:

- timestamps as RFC 3339 / ISO 8601 UTC strings
- UUIDs in canonical lowercase hyphenated form
- commit IDs as full Git object IDs unless a host system has a stronger stable identifier
- enums as short lowercase tokens like `pending`, `success`, `failed`

If a value needs structured payloads, implementations should prefer:

1. multiple related keys
2. a collection type
3. a stable serialized string format such as JSON only when necessary
