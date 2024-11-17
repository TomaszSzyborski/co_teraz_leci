import asyncio

import sentry_sdk
import uvicorn
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.routing import APIRoute, APIRouter
from starlette.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import time

from starlette.staticfiles import StaticFiles

import services.service
from services.data import programme_data
from services.service import api_router
from contextlib import asynccontextmanager

scheduler = BackgroundScheduler()


def refresh_data():
    asyncio.create_task(programme_data.refresh_data())

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(refresh_data, CronTrigger(hour=0, minute=0))
    refresh_data()
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(
    lifespan=lifespan,
    title="Co leci w telewizji?",
    openapi_url=f"/openapi.json",
)
app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)
