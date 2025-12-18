from fastapi import APIRouter, Depends, HTTPException

from app.infrastructure.github_client import GitHubClient
from app.schemas.search import SearchParameters, SearchResponse
from app.services.github_service import GitHubService

router = APIRouter()


@router.get(
    "/repositories",
    response_model=SearchResponse,
    summary="Search GitHub repositories by language and filters.",
)
async def search_repositories(
    search_params: SearchParameters = Depends(),
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
        HTTPException with status code 500 if the search fails.
    """

    try:
        github_client = GitHubClient()
        github_service = GitHubService(github_client)

        search_result = await github_service.search_and_save_repositories(
            language=search_params.lang,
            limit=search_params.limit,
            offset=search_params.offset,
            minimum_stars=search_params.stars_min,
            maximum_stars=search_params.stars_max,
            minimum_forks=search_params.forks_min,
            maximum_forks=search_params.forks_max,
        )

        return SearchResponse(
            message="CSV file created successfully",
            filename=search_result["filename"],
            download_url=f"/static/{search_result['filename']}",
        )

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(error)}",
        )
