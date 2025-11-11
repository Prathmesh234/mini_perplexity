# Backend

Python backend skeleton for Mini Perplexity.

- app/: application modules (api, core, models, services)
- tests/: backend tests

## Using uv

This backend is configured to use uv for dependency management and virtual environments.

- Create/activate environment and install deps:
  - Install: `uv sync`
  - Shell: `uv shell`
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`
- Run the server (later when implemented): `uv run uvicorn app.main:app --reload`
