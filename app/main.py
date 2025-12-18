from typing import Dict

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.endpoints import search
from app.core.config import settings

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)


app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(search.router, prefix="/api/v1/search", tags=["search"])


@app.get("/")
async def root() -> Dict[str, str | Dict[str, str]]:
    """
    Return information about the API.

    Returns a dictionary with the following information:
        - service: The name of the service, as a string.
        - version: The version of the service, as a string.
        - endpoints: A dictionary of endpoints with their paths, as strings.
    """

    info: Dict[str, str | Dict[str, str]] = {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "endpoints": {
            "search": "/api/v1/search/repositories",
            "docs": "/docs",
        },
    }

    return info


@app.get("/health", response_model=Dict[str, str])
async def health_check() -> Dict[str, str]:
    """
    Return the health status of the API.
    """

    return {"status": "healthy"}
