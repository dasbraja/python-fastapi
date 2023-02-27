import os
import requests
from dataclasses import dataclass

from pydantic import BaseModel as PydanticBaseModel
from datetime import datetime, timezone
from sqlalchemy.engine import Engine

# set env for global secret
ENV_SECRETS_PATH = os.environ.get('ENV_SECRETS_PATH')

if ENV_SECRETS_PATH and os.path.isdir(ENV_SECRETS_PATH):
    for x in os.listdir(ENV_SECRETS_PATH):
        if os.path.isfile(ENV_SECRETS_PATH + "/" + x):
            env_file = open(ENV_SECRETS_PATH + "/" + x, "r")
            os.environ[x.upper().replace('-', '_')] = env_file.read()
            env_file.close()

MYSQL_CONN_STR = os.environ.get('SIRENDB_CONN_STR', 'mysql://bdasdev@bdasdev:Sbux76579321@bdasdev.mysql.database.azure.com:3306/bdasdev')

API_PATH = os.environ.get('API_PATH', '')
NR_ENV = os.environ.get('NR_ENV', 'test')
NUM_WORKERS = int(os.environ.get('NUM_WORKERS', 64))
SSL_MODE = os.environ.get('SSL_MODE', 'False').lower() in ['true', '1']
SSL_PATH = os.environ.get('SSL_PATH', 'BaltimoreCyberTrustRoot.crt.pem')
ENABLE_USER_LOG = os.environ.get('ENABLE_USER_LOG', 'False').lower() in ['true', '1']
ALLOWED_HOSTS = [
    f'http://cr-api.default:8008{API_PATH}/',
    f'http://cr-api:8008{API_PATH}/',
    'http://testserver/',
    'http://127.0.0.1:5000/',
    'http://127.0.0.1:8008/',
    "http://bdasdev.westus.cloudapp.azure.com:8008",
    f'http://{os.getenv("POD_IP")}:8008{API_PATH}/'
]
ALLOWED_ORIGINS = ["http://localhost:3000",
                   "http://localhost:5000",
                   "https://bdasdeviot.westus.cloudapp.azure.com",
                   "https://bdasdev.westus.cloudapp.azure.com"
                   ]

def _format_datetime(dt: datetime) -> str:
    """Converts a datetime to a string with the timezone offset.

    Args:
        dt (datetime): The datetime to convert

    Returns:
        str: An iso8601 formatted string with the timezone offset
    """
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + 'Z'

    return dt.isoformat() + 'Z'


class BaseModel(PydanticBaseModel):
    class Config:
        json_encoders = {
            datetime: _format_datetime
        }


class RequestInterface:
    def get(self, url, params=None, **kwargs):
        pass

    def post(self, url, data=None, json=None, **kwargs):  # noqa
        pass


class Requests(RequestInterface):
    def get(self, url, params=None, **kwargs):
        return requests.get(url, params=params, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):  # noqa
        return requests.post(url, data=data, json=json, **kwargs)



@dataclass
class IO:
    primary_engine: Engine
    requests: RequestInterface

