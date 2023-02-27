from config import ALLOWED_ORIGINS, API_PATH
from starlette.middleware import Middleware
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from api.gp import router as gp_router
from api.zurn import router as zurn_router
from api.common import router as common_router



middleware = [
    Middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=True, allow_methods=['*'],
               allow_headers=['*'])
]

app = FastAPI(title="Connected Restroom",
              version='1.0',
              middleware=middleware,
              root_path=API_PATH,
              swagger_ui_parameters={"defaultModelsExpandDepth": -1, "displayRequestDuration": True,
                                     "docExpansion": "none", "tryItOutEnabled": True, "persistAuthorization": True,
                                     "syntaxHighlight": {"activated": False}
                                     })

app.include_router(common_router, tags=['Common Data'])
app.include_router(gp_router, tags=['Georgia Pacific Dispensers'])
app.include_router(zurn_router, tags=['Zurn Valves'])

