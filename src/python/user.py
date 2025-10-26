from typing import List, Dict, Any, Optional
from .query_builder import DataQueryBuilder, CSVData


class User:
    def __init__(self, username: str, data: CSVData, headers: List[str]):
        self.username = username
        self.data = data
        self.headers = headers
        self.saved_queries: Dict[str, List[tuple]] = dict()

    def create_query(self, query_name: str) -> DataQueryBuilder:
        builder = DataQueryBuilder(self.data, self.headers)
        setattr(builder, '_query_name', query_name)
        return builder

    def save_query(self, query_name: str, builder: DataQueryBuilder) -> None:
        operations = builder.get_operations()
        self.saved_queries[query_name] = operations

    def execute_saved_query(self, query_name: str) -> Any:
        if query_name not in self.saved_queries:
            raise KeyError(f"Сохраненный запрос '{query_name}' не найден")

        builder = DataQueryBuilder(self.data, self.headers)
        builder.set_operations(self.saved_queries[query_name])
        return builder.execute()

    def get_saved_query_names(self) -> List[str]:
        return list(self.saved_queries.keys())

    def delete_saved_query(self, query_name: str) -> bool:
        if query_name in self.saved_queries:
            del self.saved_queries[query_name]
            return True
        return False

    def has_saved_query(self, query_name: str) -> bool:
        return query_name in self.saved_queries

    def rename_saved_query(self, old_name: str, new_name: str) -> None:
        if old_name not in self.saved_queries:
            raise KeyError(f"Сохраненный запрос '{old_name}' не найден")
        if new_name in self.saved_queries:
            raise ValueError(f"Запрос с именем '{new_name}' уже существует")

        self.saved_queries[new_name] = self.saved_queries.pop(old_name)
