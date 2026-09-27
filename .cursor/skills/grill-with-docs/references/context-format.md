# CONTEXT.md Format

## File Structure

### Single Context (Most Repositories)
```
/
├── CONTEXT.md
├── docs/
│   └── adr/
│       ├── 0001-slug.md
│       └── 0002-slug.md
└── src/
```

Create files lazily: only when you have something to write. If no `CONTEXT.md` exists, create it when the first term crystallises. If no `docs/adr/` exists, create it when the first qualifying decision is made.

### Multiple Contexts (Monorepos or Multi-Domain Systems)
If a `CONTEXT-MAP.md` exists at the root, follow its context pointers:
```
/
├── CONTEXT-MAP.md
├── docs/
│   └── adr/                          ← system-wide decisions
└── src/
    ├── ingestion/
    │   ├── CONTEXT.md
    │   └── docs/adr/                 ← context-specific decisions
    └── transformation/
        ├── CONTEXT.md
        └── docs/adr/
```

---

## CONTEXT.md Template

```markdown
# {Context Name}

{One or two sentence description of what this context is and why it exists.}

## Language

**{Canonical Term}**:
{A one or two sentence description of what the term IS, not what it does.}
_Avoid_: {Synonym 1}, {Synonym 2}

**{Another Term}**:
{Precise description.}
_Avoid_: {Deprecations or ambiguous alternatives}
```

## Rules for Glossary Entries

- **Be opinionated**: When multiple synonyms exist, select the best one and list others under `_Avoid_`.
- **Keep definitions tight**: 1–2 sentences maximum. Define what it *is*, not its implementation details.
- **Project-specific vocabulary only**: General programming patterns (e.g. factory, timeout, exception) do not belong here. Only domain concepts specific to the project belong.
- **No implementation spec**: Do not treat `CONTEXT.md` as a scratchpad or technical design doc. It is strictly a ubiquitous language glossary.
