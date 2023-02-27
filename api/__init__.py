from fastapi_restful import Resource
from fastapi import Depends
from api.utils import get_io
from config import IO

class IoResource(Resource):
    io: IO = Depends(get_io)