# AI Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the progress log at `progress.txt` (check Codebase Patterns section first)
2. Open @TASKS.md Find the next task that is not yet completed
3. Implement that single task
4. Run quality checks (e.g., typecheck, lint, test - use whatever your project requires)
5. Amend, update or add additional tests/ for the function or feature that has been added / changed etc.
6. Update `progress.txt` file if you discover reusable patterns (see below)
7. If checks, tests and unit tests all pass append your progress to `progress.txt`
8. If all checks pass etc, `progress.txt` has been updated, commit ALL changes with message: `feat: [Task ID] - [Task Title]`
9. Continue to the next task until all tasks are complete and passing.


## Progress Report Format

APPEND to progress.txt (never replace, always append):
```
## [Date/Time] - [Task ID]
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered (e.g., "this codebase uses X for Y")
  - Gotchas encountered (e.g., "don't forget to update Z when changing W")
  - Useful context (e.g., "the evaluation panel is in component X")
---
```

The learnings section is critical - it helps future iterations avoid repeating mistakes and understand the codebase better.

## Consolidate Patterns

If you discover a **reusable pattern** that future iterations should know, add it to the `## Codebase Patterns` section at the TOP of progress.txt (create it if it doesn't exist). This section should consolidate the most important learnings:

```
## Codebase Patterns
- Example: Use `sql<number>` template for aggregations
- Example: Always use `IF NOT EXISTS` for migrations
- Example: Export types from actions.ts for UI components
```

Only add patterns that are **general and reusable**, not story-specific details.

## Update progress.txt Files

Before committing, check if any edited files have learnings worth preserving in nearby progress.txt files:

1. **Identify directories with edited files** - Look at which directories you modified
2. **Check for existing progress.txt** - Look for progress.txt in those directories or parent directories
3. **Add valuable learnings** - If you discovered something future developers/agents should know:
   - API patterns or conventions specific to that module
   - Gotchas or non-obvious requirements
   - Dependencies between files
   - Testing approaches for that area
   - Configuration or environment requirements

**Examples of good progress.txt additions:**
- "When modifying X, also update Y to keep them in sync"
- "This module uses pattern Z for all API calls"
- "Tests require the dev server running on PORT 3000"
- "Field names must match the template exactly"

**Do NOT add:**
- Story-specific implementation details
- Temporary debugging notes
- Information already in progress.txt

Only update progress.txt if you have **genuinely reusable knowledge** that would help future work in that directory.

## Quality Requirements

- ALL commits must pass your project's quality checks (typecheck, lint, test)
- Do NOT commit broken code
- Keep changes focused and minimal
- Follow existing code patterns

## Important

- Work on ONE story per iteration
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in progress.txt before starting

