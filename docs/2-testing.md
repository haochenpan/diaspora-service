# Testing

## Testing in Virtual Environment

### Action Provider

To test the Action Provider in a virtual environment, run:

```bash
pytest tests/action_provider_test.py

coverage erase  
coverage run -m pytest
coverage report
```


### Web Service

To test the Web Service in a virtual environment, run:

```bash
pytest tests/web_service_test.py
```

### Run `tox` for both services

```bash
source secrets.sh
tox
```
