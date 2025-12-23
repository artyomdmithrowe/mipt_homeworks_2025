from typing import Any, Optional, ClassVar, Callable

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

    _CSV_FIELD_MAPPING: ClassVar[dict[str, Callable[[Any], Any]]] = {
        "owner_login": lambda repo: repo.owner.login,
        "owner_url": lambda repo: repo.owner.html_url,
        "license_name": lambda repo: repo.license.name if repo.license else "",
    }

    @classmethod
    def get_csv_headers(cls) -> list[str]:
        """
        Returns list of CSV column headers for flat export.
        Auto-generated from model fields + custom nested fields.

        Returns:
            List of column names for CSV export
        """

        simple_fields = [
            field_name
            for field_name, field_info in cls.model_fields.items()
            if field_name not in ("owner", "license")
        ]

        custom_fields = list(cls._CSV_FIELD_MAPPING.keys())
        return simple_fields + custom_fields

    def to_csv_dict(self) -> dict[str, Any]:
        """
        Converts repository to flat dictionary for CSV export.
        Auto-generated from model fields + custom nested fields.

        Returns:
            Dictionary with flattened repository data
        """
        result = {}

        for field_name in self.model_fields.keys():
            if field_name not in ("owner", "license"):
                value = getattr(self, field_name)
                result[field_name] = value if value is not None else ""

        for field_name, extractor in self._CSV_FIELD_MAPPING.items():
            result[field_name] = extractor(self)

        return result


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
