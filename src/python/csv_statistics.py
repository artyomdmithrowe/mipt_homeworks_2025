from typing import List, Dict, Any, Optional, Union
import statistics
from collections import Counter
from .csv_reader import CSVData, CSVRow


class StatisticsCalculator:
    def __init__(self, data: CSVData):
        self.data = data
        self.stats: CSVRow = dict()

    def median_by_field(self, field: str) -> Optional[float]:
        values = [
            record[field] for record in self.data
            if isinstance(record.get(field), (int, float)) and record[field] is not None
        ]

        if not values:
            return None

        return statistics.median(values)

    def median_by_field_and_save(self, field: str) -> Optional[float]:
        median_value = self.median_by_field(field)
        self.stats[field] = median_value
        return median_value

    def top_repos_by_field(self, field: str, limit: int = 10) -> CSVData:
        valid_data = [record for record in self.data if isinstance(record.get(field), (int, float))]
        return sorted(valid_data, key=lambda x: x.get(field, 0), reverse=True)[:limit]

    def mean_by_field(self, field: str) -> Optional[float]:
        values = [
            record[field] for record in self.data
            if isinstance(record.get(field), (int, float)) and record[field] is not None
        ]

        if not values:
            return None

        return statistics.mean(values)

    def count_by_field(self, field: str) -> CSVRow:
        counter = Counter(record.get(field) for record in self.data)
        return dict(counter)

    def field_summary(self, field: str) -> CSVRow:
        values = [
            record[field] for record in self.data
            if isinstance(record.get(field), (int, float)) and record[field] is not None
        ]

        if not values:
            return {
                'count': 0,
                'min': None,
                'max': None,
                'mean': None,
                'median': None
            }

        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values)
        }

    def get_stats(self) -> CSVRow:
        return self.stats.copy()

    def clear_stats(self) -> None:
        self.stats.clear()


class UserStatisticsCalculator(StatisticsCalculator):
    def median_by_repository_size(self):
        return self.median_by_field('Size')

    def most_starred_repository(self) -> Optional[CSVRow]:
        top_repos = self.top_repos_by_field('Stars', 1)
        return top_repos[0] if top_repos else None

    def repos_without_language(self) -> CSVData:
        return [record for record in self.data if not record.get('Language')]

    def mean_starts(self):
        return self.mean_by_field('Stars')

    def smallest_repository_size(self):
        return self.field_summary('Size')['min']
