from typing import List, Dict, Any, Callable, Optional, Union
from difflib import get_close_matches
from collections import defaultdict
from functools import reduce
import operator
import json
from .csv_reader import CSVData, CSVRow

QueryData = Union[CSVData, Dict[Any, CSVData]]


class DataQueryBuilder:
    def __init__(self, data: CSVData, headers: List[str]):
        self.original_data = data
        self.headers = headers
        self._operations: List[tuple] = []
        self.optimised = True
        self.result_data: Optional[QueryData] = None

    def select(self, *fields: str) -> 'DataQueryBuilder':
        invalid_fields = [field for field in fields if field not in self.headers]
        if invalid_fields:
            suggestions = [
                f"{field}→{get_close_matches(field, self.headers, n=2, cutoff=0.6)}"
                for field in invalid_fields
            ]
            raise ValueError(f"Недопустимые поля: {invalid_fields}. Подсказки: {suggestions}")

        self._operations.append(('select', fields))
        self.optimised = False
        return self

    def filter(self, condition: Callable[[Dict[str, Any]], bool]) -> 'DataQueryBuilder':
        self._operations.append(('filter', condition))
        self.optimised = False
        return self

    def sort_by(self, field: str, reverse: bool = False) -> 'DataQueryBuilder':
        if field not in self.headers:
            raise ValueError(f"Поле {field} отсутствует")

        self._operations.append(('sort', (field, reverse)))
        self.optimised = False
        return self

    def group_by(self, field: str) -> 'DataQueryBuilder':
        if field not in self.headers:
            raise ValueError(f"Поле {field} отсутствует")

        self._operations.append(('group', field))
        self.optimised = False
        return self

    def _optimize_operations(self) -> List[tuple]:
        if self.optimised:
            return self._operations

        order = {'filter': 0, 'sort': 1, 'select': 2, 'group': 3}
        optimise_order = lambda op: order.get(op[0]) or -1

        self.optimised = True
        return sorted(self._operations, key=optimise_order)

    def _safe_grouping_key(self, item: Dict[str, Any], field: str) -> str:
        try:
            raw_value = item.get(field)
            
            if raw_value is None:
                return "__NULL__"
            elif isinstance(raw_value, (str, int, float, bool)):
                return str(raw_value)
            elif isinstance(raw_value, (list, tuple)):
                return f"__LIST__{json.dumps(raw_value, sort_keys=True, default=str)}"
            elif isinstance(raw_value, dict):
                return f"__DICT__{json.dumps(raw_value, sort_keys=True, default=str)}"
            else:
                return str(raw_value)
                
        except Exception as e:
            raise ValueError(f"Ошибка при обработке ключа группировки в записи {item}: "
                           f"поле '{field}' = {raw_value}, ошибка: {e}") from e
    
    def execute(self) -> QueryData:
        if not self._operations:
            self.result_data = self.original_data.copy()
            return self.result_data

        data = self.original_data.copy()
        grouped_result = None

        for operation_type, operation_data in self._optimize_operations():
            if operation_type == 'filter':
                data = [item for item in data if operation_data(item)]
            elif operation_type == 'sort':
                field, reverse = operation_data
                data = sorted(data, key=lambda x: (x.get(field) is None, x.get(field)), reverse=reverse)
            elif operation_type == 'select':
                data = [{field: item.get(field) for field in operation_data} for item in data]
            elif operation_type == 'group':
                grouped = defaultdict(list)
                for item in data:
                    key = self._safe_grouping_key(item, operation_data)
                    grouped[key].append(item)
                grouped_result = dict(grouped)

        self.result_data = grouped_result if grouped_result is not None else data
        return self.result_data

    def count(self) -> int:
        if self.result_data is None:
            self.execute()

        if isinstance(self.result_data, dict):
            return sum(len(group) for group in self.result_data.values())
        return len(self.result_data)

    def first(self) -> Optional[CSVRow]:
        if self.result_data is None:
            self.execute()

        if isinstance(self.result_data, dict):
            first_key = next(iter(self.result_data), None)
            return self.result_data[first_key][0] if first_key and self.result_data[first_key] else None

        return self.result_data[0] if self.result_data else None

    def get_headers(self) -> List[str]:
        return self.headers.copy()

    def get_operations(self) -> List[tuple]:
        return self._operations.copy()
    
    def set_operations(self, operations: List[tuple]) -> None:
        self._operations = operations.copy()
        self.optimised = False
