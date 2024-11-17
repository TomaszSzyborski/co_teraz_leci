from datetime import datetime, timedelta

import pytz
from fastapi import FastAPI, Form, Query, APIRouter
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import polars as pl
from starlette.responses import JSONResponse
import logging
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import configuration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from services.data import programme_data

api_router = APIRouter()
templates = Jinja2Templates(directory="templates")


@api_router.get("/", response_class=HTMLResponse)
@api_router.get("/teraz", response_class=HTMLResponse)
async def index(request: Request):
    current_time = datetime.now(tz=pytz.timezone(configuration.timezone))
    filtered_df = (programme_data.data_frame
                   .filter(pl.col('start') < current_time)
                   .filter(current_time < pl.col('koniec'))
                   .filter(pl.col('czas trwania') >= 15)
                   .filter(pl.col('czas trwania') <= 300))

    filtered_df = filtered_df.with_columns(
        pl.col('start').dt.strftime('%H:%M'),
        pl.col('koniec').dt.strftime('%H:%M'))
    return templates.TemplateResponse(request=request,
                                      name="channels.html",
                                      context={"channels": filtered_df.to_dicts()})


@api_router.get("/zaraz")
async def zaraz(request: Request):
    current_time = datetime.now(tz=pytz.timezone(configuration.timezone))
    future = current_time + pl.duration(hours=timedelta(hours=2, minutes=30).total_seconds() / 3600)
    filtered_df = (programme_data.data_frame
                   .filter(pl.col('start') > current_time)
                   .filter(pl.col('koniec') < future)
                   .filter(pl.col('czas trwania') >= 15)
                   .filter(pl.col('czas trwania') <= 300))
    filtered_df = filtered_df.with_columns(
        pl.col('start').dt.strftime('%H:%M'),
        pl.col('koniec').dt.strftime('%H:%M'))
    return templates.TemplateResponse(request=request,
                                      name="channels.html",
                                      context={"channels": filtered_df.to_dicts()})


@api_router.get("/force_refresh", include_in_schema=False)
async def force_refresh_data():
    await programme_data.refresh_data(force=True)
    return JSONResponse(content={"message": "Odświeżono listę"}, status_code=200)
