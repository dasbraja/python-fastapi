from nr import *
from logger import log
from app import app  # noqa
from api.utils import get_io

log.info(f"Newrelic app: {nr.global_settings().app_name}")


@app.on_event("startup")
def startup_event():
    get_io()
