# Домашние работы

Выполнили:

- Дмитров Артём,
- Савельев Александр,
- Ряжский Дмитрий,
- Гучиев Магомед-Башир.

## Запуск

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Примеры сценариев работы

1. С фильтрами по звёздам: bash

```bash
curl "http://localhost:8000/api/v1/search/repositories?lang=python&limit=3&stars_min=1000"
```

2. С пагинацией (offset):

```bash
# Первые 3 репозитория
curl "http://localhost:8000/api/v1/search/repositories?lang=javascript&limit=3&offset=0"

# Следующие 3 репозитория
curl "http://localhost:8000/api/v1/search/repositories?lang=javascript&limit=3&offset=3"
```

3. С несколькими фильтрами:

```bash
curl "http://localhost:8000/api/v1/search/repositories?lang=go&limit=5&stars_min=500&forks_min=100"
```
