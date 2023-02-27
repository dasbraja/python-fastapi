from config import SSL_MODE, SSL_PATH, MYSQL_CONN_STR, NUM_WORKERS

from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

private_engine = None

def get_engine() -> Engine:
    global private_engine
    if not private_engine:
        private_engine = create_engine(
            MYSQL_CONN_STR,
            poolclass=QueuePool,
            pool_size=NUM_WORKERS,
            max_overflow=8,
            pool_pre_ping=True,
            connect_args={
                'connect_timeout': 15,
                'ssl': {
                    'ssl_ca': SSL_PATH
                }
            }
        )
    return private_engine