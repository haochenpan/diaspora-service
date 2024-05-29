FROM --platform=linux/amd64 python:3.11

EXPOSE 8000/tcp

WORKDIR /diaspora-app

COPY app app
COPY pyproject.toml .

RUN pip install .

CMD [ "python", "app/app.py" ]