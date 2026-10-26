# Contributing

Changes reach `main` through pull requests, and a push to `main` deploys
the service to production unless it changes only documentation
([What merging does](#what-merging-does)). Report a vulnerability as the
[security policy](https://github.com/haochenpan/diaspora-service/blob/main/.github/SECURITY.md)
says, privately and not in an issue or a pull request.

## Set up

Create the virtual environment and install the locked dependencies and
dev tools as in [Running locally](1-running.md#set-up), then install the
Git hooks:

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

## Dependencies

`pyproject.toml` lists the service's dependencies, and two files generated
by [uv](https://docs.astral.sh/uv/) lock their versions for Python 3.14 on
Linux x86_64, the image's platform:

- `requirements.txt` locks the service's dependencies. The image and CI
  install it.
- `requirements-dev.txt` locks the dev tools, at the same versions for the
  packages the two files share. CI installs it, and the image does not.

After you change the dependencies in `pyproject.toml`, regenerate both
files from the repository root, `requirements.txt` first, since it
constrains the other:

```bash
uv pip compile pyproject.toml --python-version 3.14 --python-platform x86_64-unknown-linux-gnu --exclude-newer 2026-10-26T12:00:00Z -o requirements.txt
uv pip compile pyproject.toml --extra dev -c requirements.txt --python-version 3.14 --python-platform x86_64-unknown-linux-gnu --exclude-newer 2026-10-26T12:00:00Z -o requirements-dev.txt
```

Each file's header holds the command that generated it. uv ignores
releases published after the `--exclude-newer` time, so the commands give
the same versions on any machine, and it keeps the versions already in a
file where it can. To move every package to its newest release, add
`--upgrade` to both commands and set `--exclude-newer` to the current
date.

Dependabot proposes updates to both files once a month, in one pull
request. It edits the pinned versions in place and keeps each header as
it is, but it does not run uv: the `--exclude-newer` date stays, and a
package that an updated one newly needs is not added. If the unit tests
fail on such a pull request, regenerate the files with `--upgrade`
instead. Dependabot also proposes new builds and patch releases of the
image's Python 3.14, but not a new minor or major version of Python.

## Releases

Each change that users can see, such as a new route or a change to a
response, gets a release:

1. In the pull request that makes the change, bump `version` in
   `pyproject.toml`.
2. Once it is merged, tag the commit on `main` with `v` and the version,
   and push the tag:

    ```bash
    git tag vX.Y.Z <commit>
    git push origin vX.Y.Z
    ```

3. Publish a GitHub release from the tag, with notes that GitHub generates
   from the pull requests merged since the previous release:

    ```bash
    gh release create vX.Y.Z --verify-tag --generate-notes
    ```

The service reports its version, followed by `+` and the first 12
characters of the commit its image was built from, as `info.version` in
`/openapi.json`, and the API documentation at `/` shows it:

```bash
curl -sS https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/openapi.json | jq -r .info.version
```

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
