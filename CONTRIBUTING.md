# Contributing

Thank you for your interest in improving **ParquetDB**! The project is maintained by volunteers and thrives on an inclusive and supportive community. This guide explains how to set up a development environment, run the automated checks, and submit your changes.

## Table of Contents
- [Being Respectful](#being-respectful)
- [Getting the Code](#getting-the-code)
- [Running the Test Suite Locally](#running-the-test-suite-locally)
- [Automated Checks on Pull Requests](#automated-checks-on-pull-requests)
- [Submitting a Change](#submitting-a-change)
- [Documentation Contributions](#documentation-contributions)
- [Style Guide & Tooling](#style-guide--tooling)
- [Need Help?](#need-help)

## Being Respectful

Please demonstrate empathy and kindness toward other people, other software, and the communities who have worked diligently to build (un‑)related tools. Questions and suggestions are welcome; personal attacks and dismissive language are not. In short: **be excellent to one another**.

## Getting the Code

1. **Fork** <https://github.com/lllangWV/ParquetDB> to your own GitHub account and then **clone** your fork:

   ```bash
   git clone https://github.com/<your-username>/ParquetDB.git
   cd ParquetDB
   ```

2. **Create a virtual environment** and install the development dependencies:

   ```bash
   conda create -n parquetdb python=3.10
   conda activate parquetdb
   pip install -e .[dev]
   ```

The extra `[dev]` tag installs the testing, documentation, and formatting tools that the automated build will run.

## Running the Test Suite Locally

The full test suite (unit tests) executes automatically on every pull request. Running it locally first makes the review process faster and greener:

```bash
pytest tests/ -v  # run all Python tests
```

## Automated Checks on Pull Requests

Our continuous‑integration pipeline is powered by **GitHub Actions** and **Read the Docs**:

* **Unit tests** (`pytest`) ensure the library continues to behave as documented.
* **Docs build test** guarantees that the reStructuredText/Markdown compiles without warnings. A preview is published for every PR.

These checks are triggered automatically when you open a pull request against the **main** branch. A pull request cannot be merged until all checks show a green ✔︎.

## Submitting a Change

1. **Create a branch** off `main` with a descriptive name (e.g. `feature/fast-writer` or `bugfix/issue-123`).
2. Commit your work in logical units and push the branch to your fork.
3. Open a **pull request** (PR) on GitHub. The template will prompt you for a short description and a checklist (tests, docs, etc.).
4. Wait for the automated checks to pass and address any review comments.
5. A maintainer will squash‑merge your PR when it is ready.

## Documentation Contributions

The documentation is hosted on **Read the Docs** and built automatically—there is no need to commit generated HTML. If you modify or add documentation, include the source files only (`docs/*.rst`, `docs/examples/*.ipynb` …).

ParquetDB’s docs are generated with **Sphinx** and **sphinx‑gallery**. If you add code to the package, make sure to add the proper docstrings so the API reference is generated automatically.

To build the docs locally from the project root:

```bash
cd docs
make clean && make html
```

This cleans out `docs/_build`, regenerates the HTML, and stores it under `docs/_build/html`.

## Examples

Example tutorials live in Jupyter notebooks (`.ipynb`) under `examples/` and are rendered in the documentation via **nbsphinx**.

> **Important:** nbsphinx must *not* execute the notebooks during the CI docs build. Disable execution by adding the following key to the notebook metadata (in JupyterLab, choose **Edit ▸ Edit Notebook Metadata**):
>
> ```json
> {
>   "nbsphinx": {
>     "execute": "never"
>   }
> }
> ```

On documentation builds, the example directory is copied to `docs/source/examples` and the contents are rendered in the documentation. If you add an example, make sure to add it to the `examples/index.rst` file.

## Style Guide & Tooling

* **Black** formats Python code; **ruff** enforces import order and lint rules. Both run automatically via *pre‑commit* hooks and in CI.
* Follow the [NumPy docstring style](https://numpydoc.readthedocs.io/en/latest/format.html) so that API documentation is generated correctly.
* Keep pull requests focused—unrelated refactors make reviews harder.

## Need Help?

If you get stuck, feel free to open a **GitHub Discussion** or start a **draft PR** early so we can guide you. Thank you for helping to make ParquetDB better!

