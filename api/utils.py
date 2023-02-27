from database.primary.mysql import get_engine as get_primary_engine
from config import IO, Requests

def get_io() -> IO:
    return IO(get_primary_engine(), Requests())


