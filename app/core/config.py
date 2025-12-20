import os
from typing import Optional


class Settings:
    """
    Settings for the application.
    """

    # App settings
    APP_NAME: str = "GitHub Repository Search API"
    APP_VERSION: str = "1.0.0"

    # GitHub API settings
    GITHUB_API_URL: str = "https://api.github.com"
    GITHUB_API_VERSION: str = "2022-11-28"
    GITHUB_PER_PAGE: int = 100
    GITHUB_TOKEN: Optional[str] = os.getenv("GITHUB_TOKEN")

    # Settings static files
    STATIC_DIR: str = "static"
    CSV_FILENAME_TEMPLATE: str = "repositories_{lang}_{limit}_{offset}.csv"

    # Settings pagination
    DEFAULT_LIMIT: int = 10
    MAX_LIMIT: int = 1000
    DEFAULT_OFFSET: int = 0
    REQ_TIMEOUT: float = 30.0


settings = Settings()
