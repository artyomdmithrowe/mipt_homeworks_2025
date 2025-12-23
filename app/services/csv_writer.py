import csv
from typing import List
from pathlib import Path

import aiofiles

from app.core.config import settings
from app.schemas.search import GitHubRepository


class CsvWriter:
    @staticmethod
    async def write_repositories(
        filename: str, repositories: List[GitHubRepository]
    ) -> None:
        """
        Saves a list of repositories to a CSV file.
        Args:
            filename: Filename of the CSV file
            repositories: List of GitHubRepository objects
        Returns:
            None
        """

        static_dir = Path(settings.STATIC_DIR)
        filepath = static_dir / filename

        csv_headers = GitHubRepository.get_csv_headers()
        rows = [repo.to_csv_dict() for repo in repositories]

        async with aiofiles.open(
            filepath, mode="w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=csv_headers)
            await writer.writeheader()

            for row in rows:
                await writer.writerow(row)
