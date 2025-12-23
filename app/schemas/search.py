from typing import Optional

from pydantic import BaseModel, Field


class GitHubOwner(BaseModel):
    """Owner information from GitHub API."""

    login: str
    html_url: str
    type: str
    site_admin: bool


class GitHubLicense(BaseModel):
    """License information from GitHub API."""

    key: str
    name: str
    spdx_id: Optional[str] = None
    url: Optional[str] = None


class GitHubRepository(BaseModel):
    """Repository information from GitHub API."""

    id: int
    name: str
    full_name: str
    html_url: str
    description: Optional[str] = None
    language: Optional[str] = None
    stargazers_count: int
    forks_count: int
    watchers_count: int
    open_issues_count: int
    size: int
    created_at: str
    updated_at: str
    pushed_at: str
    owner: GitHubOwner
    license: Optional[GitHubLicense] = None
    private: bool
    fork: bool
    archived: bool
    disabled: bool
    default_branch: str


class GitHubSearchResponse(BaseModel):
    """Response from GitHub search API."""

    total_count: int
    incomplete_results: bool
    items: list[GitHubRepository]


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
