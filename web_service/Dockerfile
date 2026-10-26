FROM --platform=linux/amd64 python:3.15@sha256:67ee282bdbe5ece1e38ea94fa68f1420e969e6176e6c826b9acc931055011bfb

EXPOSE 8000/tcp

WORKDIR /diaspora-web-service

COPY web_service web_service
COPY pyproject.toml requirements.lock ./

RUN pip install -r requirements.lock && pip install --no-deps .

# CMD [ "uvicorn", "web_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload" ]
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "-k", "uvicorn.workers.UvicornWorker", "web_service.main:app"]
