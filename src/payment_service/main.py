from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from payment_service.api.routes import router
from payment_service.config import get_settings
from payment_service.db import engine


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(title=get_settings().app_name, version="1.0.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    return {"status": "ok"}
