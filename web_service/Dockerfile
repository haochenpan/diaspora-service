FROM --platform=linux/amd64 python:3.14.9-slim@sha256:35caba83af2fe6da9a5d922f9b0156f21d1246a48278c58a08bd253303ba42e8

EXPOSE 8000/tcp

WORKDIR /diaspora-web-service

# The locked dependencies first, so a change to the code alone reuses this
# layer
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml ./
COPY web_service web_service
RUN pip install --no-cache-dir --no-deps .

RUN useradd --create-home diaspora
USER diaspora

# The commit the image is built from; the service adds it to its version
ARG GIT_SHA=
ENV GIT_SHA=$GIT_SHA

# One worker, since consumer instances live in its memory. On SIGTERM,
# gunicorn gives the worker 30 s to finish requests and shut down before
# it kills it.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "-k", "uvicorn_worker.UvicornWorker", "--graceful-timeout", "30", "web_service.main:app"]
