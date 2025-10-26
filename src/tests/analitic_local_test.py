from src.python import *

# Данные тесты написан с целью локальной отладки классов


def test_csv_reader():
    print("=== Тест CSVReader ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()
    headers = reader.get_headers()

    print(f"Загружено записей: {len(data)}")
    print(f"Заголовки: {headers}")
    print(f"Первая запись: {data[0] if data else 'Нет данных'}")
    print()


def test_query_builder():
    print("=== Тест DataQueryBuilder ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()
    headers = reader.get_headers()

    builder = DataQueryBuilder(data, headers)
    result = builder.select("Name", "Stars", "Language").filter(
        lambda x: x.get("Language") == "Python"
    ).sort_by("Stars", reverse=True).execute()

    print(f"Python репозиториев: {len(result)}")
    print("Топ-3 Python репозитория:")
    for i, repo in enumerate(result[:3], 1):
        print(f"  {i}. {repo['Name']} - {repo.get('Stars', 0)} stars")
    print()


def test_user_saved_queries():
    print("=== Тест User с сохраненными запросами ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()
    headers = reader.get_headers()

    user = User("test_user", data, headers)

    builder = user.create_query("top_js_repos")
    builder.select("Name", "Stars", "Language").filter(
        lambda x: x.get("Language") == "JavaScript"
    ).sort_by("Stars", reverse=True)

    user.save_query("top_js_repos", builder)

    result = user.execute_saved_query("top_js_repos")
    print(f"Сохраненный запрос 'top_js_repos': {len(result)} JavaScript репозиториев")
    print(f"Лучший JS репозиторий: {result[0]['Name']} - {result[0]['Stars']} stars")
    print()


def test_statistics():
    print("=== Тест StatisticsCalculator ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()

    stats = UserStatisticsCalculator(data)

    median_stars = stats.median_by_field("Stars")
    most_starred = stats.most_starred_repository()
    no_language = stats.repos_without_language()
    top_10_stars = stats.top_repos_by_field("Stars", 10)

    print(f"Медиана звезд: {median_stars}")
    print(f"Самый залайканный: {most_starred['Name']} - {most_starred['Stars']} stars")
    print(f"Репозиториев без языка: {len(no_language)}")
    print("Топ-5 по звездам:")
    for i, repo in enumerate(top_10_stars[:5], 1):
        print(f"  {i}. {repo['Name']} - {repo['Stars']} stars")
    print()


def test_exporter():
    print("=== Тест StatisticsExporter ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()

    stats = StatisticsCalculator(data)
    top_repos = stats.top_repos_by_field("Stars", 5)

    json_output = StatisticsExporter.export_to_json(top_repos)
    csv_output = StatisticsExporter.export_to_csv(top_repos)

    print("JSON экспорт (первые 200 символов):")
    print(json_output[:200] + "...")
    print()
    print("CSV экспорт (первые 200 символов):")
    print(csv_output[:200] + "...")
    print()


def test_group_by():
    print("=== Тест группировки ===")
    reader = CSVReader("../../homework_oop/repositories.csv")
    data = reader.read().get_data()
    headers = reader.get_headers()

    builder = DataQueryBuilder(data, headers)
    result = builder.group_by("Language").execute()

    top_languages = sorted([(lang, len(repos)) for lang, repos in result.items()],
                           key=lambda x: x[1], reverse=True)[:5]

    print("Топ-5 языков по количеству репозиториев:")
    for lang, count in top_languages:
        print(f"  {lang}: {count} репозиториев")
    print()


if __name__ == "__main__":
    test_csv_reader()
    test_query_builder()
    test_user_saved_queries()
    test_statistics()
    test_exporter()
    test_group_by()

    print("=== Все тесты завершены ===")
    