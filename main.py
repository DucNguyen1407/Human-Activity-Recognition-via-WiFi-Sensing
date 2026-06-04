from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import asyncio

from app.api import sessions
from app.api import ws
from app.api import config_scenario
from app.api import camera
from app.api import ethernet
from app.api import uart
from app.services.recording_service import set_main_loop

app = FastAPI()

app.include_router(sessions.router, prefix="/sessions")
app.include_router(config_scenario.router)
app.include_router(ws.router)
app.include_router(camera.router)
app.include_router(ethernet.router)
app.include_router(uart.router)

app.mount("/static", StaticFiles(directory="app/ui/static"), name="static")

templates = Jinja2Templates(directory="app/ui/templates")


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    set_main_loop(loop)


@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={}
    )