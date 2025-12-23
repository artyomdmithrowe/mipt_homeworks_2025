import httpx

from app.core.config import settings
from app.schemas.search import GitHubRepository, GitHubSearchResponse


class GitHubClient:
    """
    Client for interacting with GitHub API.
    """

    def __init__(self) -> None:
        self.base_url = settings.GITHUB_API_URL
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": settings.GITHUB_API_VERSION,
            "User-Agent": "MIPT-FastAPI-Search",
        }

        if settings.GITHUB_TOKEN is not None:
            self.headers["Authorization"] = f"Bearer {settings.GITHUB_TOKEN}"

        self.client = httpx.AsyncClient(
            timeout=settings.REQ_TIMEOUT, headers=self.headers, base_url=self.base_url
        )

    async def search_repositories(
        self, search_query: str, page_number: int = 1, results_per_page: int = 100
    ) -> GitHubSearchResponse:
        """
        Searches GitHub repositories based on the given query.

        Args:
            search_query: GitHub search query
            page_number: Page number to fetch
            results_per_page: Number of results per page

        Returns:
            GitHubSearchResponse with structured repository data
        """

        url = f"{self.base_url}/search/repositories"
        params = {
            "q": search_query,
            "page": page_number,
            "per_page": min(results_per_page, settings.GITHUB_PER_PAGE),
            "sort": "stars",
            "order": "desc",
        }

        resp = await self.client.get(url, params=params, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        return GitHubSearchResponse.model_validate(data)

    async def search_repositories_paginated(
        self, query: str, limit: int, offset: int = 0
    ) -> list[GitHubRepository]:
        """
        Fetch multiple pages of results to reach desired limit.

        Args:
            query: GitHub search query
            limit: Total number of repositories to fetch
            offset: Number of repositories to skip

        Returns:
            List of GitHubRepository objects
        """

        repositories: list[GitHubRepository] = []
        page_number: int = 1

        while len(repositories) < offset + limit:
            try:
                page_data: GitHubSearchResponse = await self.search_repositories(
                    search_query=query,
                    page_number=page_number,
                    results_per_page=settings.GITHUB_PER_PAGE,
                )

                page_repositories: list[GitHubRepository] = page_data.items
                if not page_repositories:
                    break

                repositories.extend(page_repositories)
                page_number += 1

                if len(page_repositories) < settings.GITHUB_PER_PAGE:
                    break

            except httpx.HTTPError as error:
                print(f"Error fetching page {page_number}: {error}")
                break

        start_index: int = min(offset, len(repositories))
        end_index: int = min(offset + limit, len(repositories))
        return repositories[start_index:end_index]
