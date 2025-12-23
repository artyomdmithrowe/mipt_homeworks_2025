from fastapi import APIRouter, Depends, HTTPException

from app.infrastructure.github_client import GitHubClient
from app.schemas.search import SearchParameters, SearchResponse
from app.services.github_service import GitHubService

router = APIRouter()


def get_github_client() -> GitHubClient:
    """
    Returns an instance of the GitHubClient class.

    The GitHubClient class is used to make requests to the GitHub API.

    Returns:
        GitHubClient: An instance of the GitHubClient class.
    """

    return GitHubClient()


def get_github_service(client: GitHubClient = Depends(get_github_client)) -> GitHubService:
    """
    Returns an instance of the GitHubService class.

    The GitHubService class is used to interact with the GitHub API and to save
    the search results to a CSV file.

    Args:
        client: GitHubClient instance used to make requests to the GitHub API

    Returns:
        GitHubService: An instance of the GitHubService class
    """

    return GitHubService(client)


@router.get(
    "/repositories",
    response_model=SearchResponse,
    summary="Search GitHub repositories by language and filters.",
)
async def search_repositories(
    search_params: SearchParameters = Depends(),
    github_service: GitHubService = Depends(get_github_service),
) -> SearchResponse:
    """
    Searches GitHub repositories by language and filters,
    and saves the results to a CSV file.

    Args:
        search_params: SearchParameters object containing the search parameters

    Returns:
        SearchResponse object containing the filename of the created CSV file
        and the download URL.

    Raises:
        HTTPException(404): If no repositories are found.
        HTTPException(500): If an unexpected error occurs.
    """

    try:
        search_result = await github_service.search_and_save_repositories(
            language=search_params.lang,
            limit=search_params.limit,
            offset=search_params.offset,
            minimum_stars=search_params.stars_min,
            maximum_stars=search_params.stars_max,
            minimum_forks=search_params.forks_min,
            maximum_forks=search_params.forks_max,
        )
    except ValueError as error:
        raise HTTPException(
            status_code=404,
            detail=str(error),
        )
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(error)}",
        )

    return SearchResponse(
        message="CSV file created successfully",
        filename=search_result["filename"],
        download_url=f"/static/{search_result['filename']}",
    )
