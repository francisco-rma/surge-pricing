import logging

from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

from app.dash_app import app_dash
from app.driver_position.endpoints import \
    router as driver_position_count_router

# Set up logging
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


app = FastAPI()


@app.get("/")
async def main_route():
    return {"message": "Hey, It is me Goku"}


app.include_router(
    driver_position_count_router,
    prefix="/api/driver_position_count",
    tags=["driver_position_count"],
)

app.mount("/dash", WSGIMiddleware(app_dash.server))
