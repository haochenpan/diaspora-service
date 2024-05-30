FROM --platform=linux/amd64 python:3.11

EXPOSE 8000/tcp

WORKDIR /diaspora-web-service

COPY web_service web_service
COPY common common
COPY pyproject.toml .

RUN pip install .

CMD [ "uvicorn", "web_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload" ]
