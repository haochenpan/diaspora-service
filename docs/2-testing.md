# Testing

## Testing in Virtual Environment

### Web Service

To test the Web Service in a virtual environment, run:

```bash
pytest tests/ -vv -s
```

With coverage:

```bash
coverage erase
coverage run -m pytest tests/
coverage report
```

### Run `tox`

```bash
source secrets2.sh  # pragma: allowlist secret
tox
```
