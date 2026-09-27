---
name: grill-with-docs
description: "Grilling session that challenges a plan or design against the existing domain model, sharpens terminology, and updates documentation (CONTEXT.md, ADRs) inline as decisions crystallise. Use when the user wants to stress-test a plan, says 'grill me with docs', 'grill on the design', 'challenge this against our context', or invokes /grill-with-docs."
disable-model-invocation: true
---

# Grill With Docs

Conduct a relentless interview to sharpen a plan or design against the project's existing domain language and documented architectural decisions. Update documentation (`CONTEXT.md` and ADRs) inline as decisions crystallise.

## Mental Model & Rules

1. **Design Tree**: Every decision branches into the decisions that hang off it.
2. **Work in Rounds**: The **frontier** is every decision whose prerequisites are settled: the questions you can ask *now* without guessing at answers you haven't heard yet. Ask the whole frontier in one round: number each question and provide your recommended answer. Wait for feedback before proceeding to the next round.
3. **Finding Facts is Your Job**: When a frontier question needs a fact from the codebase/filesystem, read the code directly. Never ask the user for anything you could inspect yourself.
4. **Decisions are the User's**: Present choices clearly, give a concrete recommendation, and wait for confirmation.

## Format for Rounds

```markdown
❓ **Q1** - **<Question Title>**: <Question body, context, trade-offs, multiple choices if applicable>

➡️ **Recommendation**: <Your recommended answer and rationale>

---

❓ **Q2** - **<Question Title>**: <Question body, context, trade-offs, multiple choices if applicable>

➡️ **Recommendation**: <Your recommended answer and rationale>
```

---

## Domain Modeling Discipline (Inline Documentation)

### 1. Challenge Against the Glossary
When the user uses a term that conflicts with existing language in `CONTEXT.md`, call it out immediately:
> *"Your glossary defines 'X' as A, but you seem to mean B. Which is it?"*

### 2. Sharpen Fuzzy Language
When vague or overloaded terms appear, propose a precise canonical term:
> *"You're saying 'record' — do you mean a VariantSite or a SampleCall? Those are distinct concepts in this pipeline."*

### 3. Discuss Concrete Scenarios
Stress-test domain relationships with edge-case scenarios to expose boundaries between concepts.

### 4. Cross-Reference with Code
Verify user assertions against the codebase. Surface contradictions immediately:
> *"The code currently handles multiallelic sites by doing X, but your plan assumes Y. Which is the target behavior?"*

### 5. Update `CONTEXT.md` Inline
The moment a term or concept boundary is resolved, update `CONTEXT.md` immediately in that turn.
- Do not batch updates at the end of the session.
- `CONTEXT.md` is a glossary of project terms, not an implementation spec or scratchpad.
- See `references/context-format.md` for exact formatting and rules.

### 6. Record ADRs Sparingly
Only offer to create an Architecture Decision Record (in `docs/adr/`) when **all three** conditions are met:
1. **Hard to reverse**: The cost of changing course later is substantial.
2. **Surprising without context**: A future reader will wonder *"why was it built this way?"*
3. **The result of a real trade-off**: There were viable alternatives and one was chosen for specific reasons.

If any condition is missing, skip the ADR. See `references/adr-format.md` for structure and template.

---

## Session Completion
The interview completes when the frontier is empty: every branch of the design tree is resolved, terms are captured in `CONTEXT.md`, and any qualifying ADRs are recorded. Do not jump to implementation until the user confirms shared understanding.
