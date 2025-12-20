SRC_FOLDERS=$(wildcard homework_*)

lint-check:
	ruff check $(SRC_FOLDERS)
# 	flakeheaven lint $(SRC_FOLDERS)
	mypy $(SRC_FOLDERS)
	@make lint-format

format:
	ruff format $(SRC_FOLDERS)

fix:
	ruff check $(SRC_FOLDERS) --fix

lint-format:
	ruff format $(SRC_FOLDERS) --check
