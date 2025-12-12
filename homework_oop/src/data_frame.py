from dataclasses import asdict, dataclass, fields
from typing import Any, Protocol, Self, TypeVar

from homework_oop.src.dto import GithubRepoDTO
from homework_oop.src.repositories import BaseRepository

T = TypeVar("T")


class FieldsDoesNotExistsError(Exception): ...


class DataIsEmptyError(Exception): ...


@dataclass(kw_only=True)
class DataFrame(Protocol[T]):
    repository: BaseRepository
    _data: list[T] | None = None

    def __post_init__(self): ...

    def select(self, *args, **kwargs): ...

    def sort(self, *args, **kwargs): ...

    def groub_by(self, *args, **kwargs):
        pass


@dataclass(kw_only=True)
class GitHubDataFrame(DataFrame[dict[str, Any]]):
    def __post_init__(self):
        if not self._data:
            github_repos = self.repository.read(GithubRepoDTO)
            self._data = list(map(asdict, github_repos))

    def select(self, fields_names: list[str]) -> Self:
        model_fields = [field.name for field in fields(GithubRepoDTO)]
        if len(set(fields_names).intersection(model_fields)) != len(fields_names):
            raise FieldsDoesNotExistsError(f"{fields_names=} not exists in data.")
        if not self._data:
            raise DataIsEmptyError
        result = []
        for github_repo in self._data:
            result.append(
                {
                    field: value
                    for field, value in github_repo.items()
                    if field in fields_names
                }
            )
        return self.__class__(repository=self.repository, _data=result)

    def all(self) -> list[dict[str, Any]] | None:
        return self._data
