from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self


@dataclass
class BaseDTO:
    @classmethod
    @abstractmethod
    def build(cls, row: Any) -> Self: ...


@dataclass
class GithubRepoDTO(BaseDTO):
    name: str
    description: str
    updated_at: datetime
    stars: int
    forks: int
    issues: int
    has_wiki: bool
    language: str | None = None

    @classmethod
    def build(cls, row: list[str]) -> Self:
        updated_at = row[4].replace("T", " ").replace("Z", "")
        return cls(
            name=row[0],
            description=row[1],
            updated_at=datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S"),
            stars=int(row[7]),
            forks=int(row[8]),
            issues=int(row[9]),
            has_wiki=bool(row[17]),
            language=row[11],
        )
