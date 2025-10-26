import os
from typing import List, Dict, Any, Optional, Tuple
import csv

CSVRow = Dict[str, Any]
CSVData = List[CSVRow]


class CSVReader:
    def __init__(self, filepath: str = None, delimiter: str = ','):
        self.filepath = filepath
        self.delimiter = delimiter
        self.headers: List[str] = list()
        self.data: CSVData = list()

    def _read_from_file(self) -> None:
        if not self.filepath:
            raise ValueError("Путь к файлу не указан.")

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"CSV-файл не найден: {self.filepath}")

        encodings = ('utf-8', 'utf-8-sig', 'latin-1', 'cp1252')

        for encoding in encodings:
            try:
                with open(self.filepath, 'r', encoding=encoding, newline='') as f:
                    self._parse_csv_data(f)
                return
            except UnicodeDecodeError:
                continue

        with open(self.filepath, 'r', encoding='utf-8', errors='replace') as f:
            self._parse_csv_data(f)

    def _read_from_string(self, data_string: str) -> None:
        lines = data_string.strip().splitlines()
        self._parse_csv_data(lines)

    def _parse_csv_data(self, lines: Any) -> None:
        reader = csv.reader(lines, delimiter=self.delimiter, skipinitialspace=True)

        try:
            self.headers = [header.strip() for header in next(reader)]
        except StopIteration:
            raise ValueError("CSV пуст.")

        self.data = list()
        for line_num, row in enumerate(reader, start=2):
            if not row: continue

            normalized_row = row + [''] * (len(self.headers) - len(row))

            row_dict: CSVRow = dict()
            for i, header in enumerate(self.headers):
                value = normalized_row[i].strip()
                row_dict[header] = self._convert_value(value)

            self.data.append(row_dict)

    @staticmethod
    def _convert_value(value: str) -> Any:
        if not value:
            return None

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        return value

    def read(self, filepath: Optional[str] = None, data_string: Optional[str] = None) -> 'CSVReader':
        if filepath:
            self.filepath = filepath
        if not self.filepath and not data_string:
            raise ValueError('Необходимо указать filepath или data_string')

        try:
            if data_string:
                self._read_from_string(data_string)
            else:
                self._read_from_file()
        except csv.Error as e:
            raise csv.Error(f"Ошибка загрузки CSV: {e}") from e

        return self

    def get_data(self) -> CSVData:
        return self.data.copy()

    def get_headers(self) -> List[str]:
        return self.headers.copy()

    def get_column(self, column_name: str) -> Tuple[Any]:
        if column_name not in self.headers:
            raise KeyError(f"Столбец '{column_name}' не найден.")

        return tuple(row.get(column_name) for row in self.data)

    def get_row(self, index: int) -> CSVRow:
        if index < 0:
            index += len(self.data)

        if index < 0 or index >= len(self.data):
            raise IndexError(f"Индекс строки {index} вне допустимых значений.")

        return self.data[index].copy()

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, index: int) -> CSVRow:
        return self.get_row(index)

    def __iter__(self):
        return iter(self.data)
