# Contributing to skycoll

Thanks for your interest in contributing! Here's how to get started.

## Setup

```bash
git clone https://github.com/j4ckxyz/skycoll.git
cd skycoll
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
```

## Running tests

```bash
pytest tests/ -v
```

## Adding a new command

1. Create a new module in `skycoll/commands/` (e.g. `mycommand.py`) with a `run()` function.
2. Import and wire it into the argparse setup in `skycoll/__main__.py`.
3. Add any necessary API wrappers to `skycoll/api.py`.
4. Add storage helpers to `skycoll/storage.py` if the command writes data.
5. Write tests in `tests/`.
6. Update `README.md` with usage examples.

## Code style

- Use docstrings on all public functions.
- Follow the existing import ordering convention (stdlib → third-party → local).
- Run `python -m py_compile skycoll/` before committing.

## Submitting changes

1. Fork the repo and create a feature branch.
2. Make your changes with clear commit messages.
3. Ensure all tests pass.
4. Open a pull request against `main`.