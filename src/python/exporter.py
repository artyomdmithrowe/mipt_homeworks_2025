import json
import csv
import io
from typing import Any, Optional
from .csv_reader import CSVData


class StatisticsExporter:
    @staticmethod
    def export_to_json(data: Any, indent: int = 2, ensure_ascii: bool = True) -> Optional[str]:
        if not data:
            return None

        try:
            return json.dumps(data, indent=indent, ensure_ascii=ensure_ascii, default=str)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Ошибка сериализации JSON: {e}") from e

    @staticmethod
    def save_as_json(data: Any, filepath: str, indent: int = 2, ensure_ascii: bool = True) -> None:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii, default=str)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Ошибка сериализации JSON: {e}") from e
        except IOError as e:
            raise IOError(f"Ошибка записи в файл {filepath}: {e}") from e

    @staticmethod
    def export_to_csv(data: CSVData, delimiter: str = ',') -> Optional[str]:
        if not data:
            return None

        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise TypeError("Данные должны быть списком словарей")

        fieldnames = sorted({key for record in data for key in record.keys()})

        if not fieldnames:
            return None

        output = io.StringIO()

        try:
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter=delimiter,
                quoting=csv.QUOTE_MINIMAL,
                extrasaction='ignore'
            )
            writer.writeheader()
            writer.writerows(data)

            return output.getvalue()
        except csv.Error as e:
            raise ValueError(f"Ошибка записи CSV: {e}") from e
        finally:
            output.close()

    @staticmethod
    def save_as_csv(data: CSVData, filepath: str, delimiter: str = ',') -> None:
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise TypeError("Данные должны быть списком словарей")

        fieldnames = sorted({key for record in data for key in record.keys()})

        if not fieldnames:
            raise ValueError("Отсутствуют заголовки для записи.")

        try:
            with open(filepath, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    delimiter=delimiter,
                    quoting=csv.QUOTE_MINIMAL,
                    extrasaction='ignore'
                )
                writer.writeheader()
                writer.writerows(data)
        except csv.Error as e:
            raise ValueError(f"Ошибка записи CSV: {e}") from e
        except IOError as e:
            raise IOError(f"Ошибка записи в файл {filepath}: {e}") from e
