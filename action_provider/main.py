from flask import Flask
from action_provider import __version__
app = Flask(__name__)


@app.route('/')
def hello_world():
    return f"Hello, World from Action Provider {__version__}"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
