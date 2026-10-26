# Contributing

Changes reach `main` through pull requests, and a push to `main` deploys
the service to production unless it changes only documentation
([What merging does](#what-merging-does)). Report a vulnerability as the
[security policy](https://github.com/haochenpan/diaspora-service/blob/main/.github/SECURITY.md)
says, privately and not in an issue or a pull request.

## Set up

Create the virtual environment and install the dev extras as in
[Running locally](1-running.md#set-up), then install the Git hooks:

```bash
pre-commit install
```

To work on the docs, also install the docs extras and build the site.
`docs.yml` runs the same build on every pull request and fails on any
warning:

```bash
pip install -e ".[docs]"
mkdocs build --strict
```

To preview the site while you edit, run
`mkdocs serve --dev-addr 127.0.0.1:8001`. It serves the site at
<http://127.0.0.1:8001/> and rebuilds it on every change. Without
`--dev-addr` it takes port 8000, the port the service uses in
[Running locally](1-running.md).

## Checks

The pre-commit hooks in `.pre-commit-config.yaml` run on each commit, and
pre-commit.ci runs them on every pull request without pushing fixes. To
run them on every file:

```bash
pre-commit run --all-files
```

They check whitespace and line endings, large files, merge-conflict
markers, the syntax of JSON, YAML and TOML files, the names of test files,
spelling (codespell), Python linting and formatting (ruff), types (mypy),
the GitHub workflows (actionlint, with shellcheck on their scripts), and
committed secrets (detect-secrets). The ruff settings in `pyproject.toml`
ask for lines of at most 79 characters, single quotes, Google-style
docstrings, one import per line, and `from __future__ import annotations`
in every module. The mypy settings there ask for type annotations on every
function outside the tests.

detect-secrets can flag a value that is not a secret, such as a checksum.
Mark such a line with `# pragma: allowlist secret` in code, and with
`<!-- pragma: allowlist secret -->` in Markdown, where the HTML comment
keeps the pragma off the published page.

## Tests

[Testing](2-testing.md) describes both test sets and how to run them.

- The unit tests are every test not marked `integration`. They run on
  every pull request and before every deploy, with no AWS credentials and
  with the network blocked: a unit test that opens a network connection
  fails. Before you open a pull request, run them.
- The live tests are the tests marked `integration`. They create real IAM
  users, DynamoDB items and Kafka topics, and they run only on pushes to
  `main`, nightly and by hand, as the live tests' own IAM identity
  ([The live tests' AWS identity](2-testing.md#the-live-tests-aws-identity)).
  A pull request does not run them. Someone with write access to the
  repository can run them on a branch with
  `gh workflow run tests.yml --ref <branch>`.

Name test files `*_test.py`. A test that needs AWS or Kafka must be marked
with `@pytest.mark.integration`, or be in one of the modules that
`tests/conftest.py` marks as a whole. A live test that creates an IAM
user, a namespace, a topic or a consumer group takes its names from
`live_subject()` or `live_namespace()` in `tests/live_identity`, or from
the `random_subject` fixture in `tests/conftest.py`, which calls
`live_subject()`. The live tests' IAM identity can touch only
resources with those names, and the nightly sweep, a script that deletes
what live runs leave behind, finds only those
([Naming live-test resources](2-testing.md#naming-live-test-resources)).

## Commits and pull requests

- Branch from `main`, and name the branch after the kind of change and
  its subject, such as `fix/consistent-reads`, `ci/split-unit-and-live-tests`
  or `docs/deploy-guide-and-runbook`. The prefixes in use are `feat/`,
  `fix/`, `ci/`, `docs/` and `chore/`.
- Write the commit subject and the pull request title as an imperative
  sentence that says what changes, such as "Read DynamoDB items with
  strong consistency".
- In the pull request description, say what changed and why, and whether
  merging deploys the service.
- Pull requests are squash-merged: the title, followed by the pull
  request number, becomes the subject of the one commit on `main`.

## What merging does

A merge is a push to `main`, which starts these workflows:

- `deployment.yml` runs the unit tests and then deploys the service to
  production, unless every changed file is under `docs/`, is a Markdown
  file, or is `mkdocs.yml`
  ([What triggers a deploy](3-deploying.md#what-triggers-a-deploy)).
  A deploy restarts the service, which drops every consumer instance.
- `tests.yml` runs the unit tests and the live tests.
- `docs.yml` builds the docs and publishes them to GitHub Pages.
