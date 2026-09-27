# ADR Format

Architecture Decision Records (ADRs) live in `docs/adr/` and use sequential zero-padded numbering: `0001-slug.md`, `0002-slug.md`, etc.

Create the `docs/adr/` directory lazily: only when the first qualifying ADR is accepted.

---

## Template

```markdown
# {Short Title of the Decision}

{1-3 sentences: what is the context, what did we decide, and why.}
```

Most ADRs should be 1–2 paragraphs. The value is capturing *that* a decision was made and *why*, avoiding speculative documentation.

## Optional Sections (Include only if they add genuine value)

- **Status**: `proposed | accepted | deprecated | superseded by ADR-NNNN`
- **Considered Options**: The rejected alternatives and why they were not chosen.
- **Consequences**: Key trade-offs or downstream effects to keep in mind.

---

## When to Offer an ADR

All three criteria must be true:
1. **Hard to reverse**: The cost of changing later is high.
2. **Surprising without context**: A reasonable future developer would question why this choice was made.
3. **The result of a real trade-off**: There were valid alternatives evaluated.

### Examples of Valid ADR Topics
- Core architecture (e.g., streaming directly from VCF to Arrow Parquet rather than intermediate TSV).
- Storage representation (e.g., nested sample struct arrays vs. melted wide table).
- Framework/engine choices carrying migration costs (e.g., DuckDB + dbt vs. Spark/BigQuery).
- Explicit negative decisions (e.g., "We will not store sample-level genotypes in individual columns").
