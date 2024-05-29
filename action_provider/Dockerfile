FROM --platform=linux/amd64 python:3.11

EXPOSE 8000/tcp

WORKDIR /diaspora-action-provider

COPY action_provider action_provider
COPY pyproject.toml .

RUN pip install .

CMD [ "python", "action_provider/main.py" ]