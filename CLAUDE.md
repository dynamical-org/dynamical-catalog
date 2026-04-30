# Repo guidance for Claude

## Pre-commit checklist

Always run all of the following before committing, and fix any issues
they surface:

```bash
uv run --extra dev ruff format .
uv run --extra dev ruff check .
uv run --extra dev mypy src
uv run --extra dev pytest
```

`ruff format` rewrites files in place — run it before `ruff check` so
the check sees the formatted source. Don't skip any of these (e.g. with
`--no-verify`); fix the underlying issue instead.
