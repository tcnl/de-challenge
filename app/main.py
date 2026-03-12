from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from app.services.ingestion import IngestionManager

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = IngestionManager()
    await manager.start()
    app.state.ingestion_manager = manager
    try:
        yield
    finally:
        await manager.shutdown()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(router)
