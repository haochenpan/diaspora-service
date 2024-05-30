from fastapi import FastAPI
from web_service import __version__

app = FastAPI()


@app.get("/")
async def root():
    return {"message": f"Hello World from Web Servic {__version__}"}
