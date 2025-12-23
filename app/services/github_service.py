from typing import Dict, Optional

from app.core.config import settings
from app.infrastructure.github_client import GitHubClient
from app.services.csv_writer import CsvWriter


class GitHubService:
    """Service for searching and saving repositories to CSV."""

    def __init__(self, client: GitHubClient, writer: CsvWriter | None = None):
        self.client = client
        self.writer = writer or CsvWriter()

    def _build_search_query(
        self,
        language: str,
        minimum_stars: Optional[int] = None,
        maximum_stars: Optional[int] = None,
        minimum_forks: Optional[int] = None,
        maximum_forks: Optional[int] = None,
    ) -> str:
        """
        Builds a GitHub search query based on the given parameters.

        Args:
            language: Programming language (e.g., python, javascript)
            minimum_stars: Minimum number of stars (inclusive)
            maximum_stars: Maximum number of stars (inclusive)
            minimum_forks: Minimum number of forks (inclusive)
            maximum_forks: Maximum number of forks (inclusive)

        Returns:
            A GitHub search query string
        """

        query_parts = [f"language:{language}"]

        if minimum_stars is not None:
            query_parts.append(f"stars:>={minimum_stars}")
        if maximum_stars is not None:
            query_parts.append(f"stars:<={maximum_stars}")
        if minimum_forks is not None:
            query_parts.append(f"forks:>={minimum_forks}")
        if maximum_forks is not None:
            query_parts.append(f"forks:<={maximum_forks}")

        return " ".join(query_parts)

    async def search_and_save_repositories(
        self,
        language: str,
        limit: int,
        offset: int = 0,
        minimum_stars: Optional[int] = None,
        maximum_stars: Optional[int] = None,
        minimum_forks: Optional[int] = None,
        maximum_forks: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Main method: search repositories and save to CSV.

        Args:
            language: Programming language (e.g., python, javascript)
            limit: Total number of repositories to fetch
            offset: Number of repositories to skip
            minimum_stars: Minimum number of stars (inclusive)
            maximum_stars: Maximum number of stars (inclusive)
            minimum_forks: Minimum number of forks (inclusive)
            maximum_forks: Maximum number of forks (inclusive)

        Returns:
            Dict with filename of created CSV
        """

        search_query = self._build_search_query(
            language=language,
            minimum_stars=minimum_stars,
            maximum_stars=maximum_stars,
            minimum_forks=minimum_forks,
            maximum_forks=maximum_forks,
        )

        repositories = await self.client.search_repositories_paginated(
            query=search_query,
            limit=limit,
            offset=offset,
        )

        if not repositories:
            raise ValueError(f"No repositories found for query: {search_query}")

        filename = settings.CSV_FILENAME_TEMPLATE.format(
            lang=language,
            limit=limit,
            offset=offset,
        )

        await self.writer.write_repositories(filename=filename, repositories=repositories)

        return {
            "filename": filename,
            "repositories_count": len(repositories),
        }
