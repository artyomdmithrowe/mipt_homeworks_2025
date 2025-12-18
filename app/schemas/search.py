from typing import Optional

from pydantic import BaseModel, Field


class SearchParameters(BaseModel):
    """
    Parameters for searching GitHub repositories.
    Matches all requirements from the task description.
    """

    lang: str = Field(..., description="Programming language (e.g., python, html)")
    limit: int = Field(
        default=10, ge=1, le=1000, description="Number of repositories to return"
    )
    offset: int = Field(default=0, ge=0, description="Pagination offset")
    stars_min: Optional[int] = Field(default=None, ge=0, description="Minimum stars")
    stars_max: Optional[int] = Field(default=None, ge=0, description="Maximum stars")
    forks_min: Optional[int] = Field(default=None, ge=0, description="Minimum forks")
    forks_max: Optional[int] = Field(default=None, ge=0, description="Maximum forks")


class SearchResponse(BaseModel):
    """
    Response for searching GitHub repositories.
    Matches all requirements from the task description.
    """

    message: str
    filename: str
    download_url: str
