
.PHONY: lint
lint:
	poetry run isort .
	poetry run black . --check
	poetry run pflake8 .
	poetry run mypy .

.PHONY: lint-fix
lint-fix:
	poetry run isort .
	poetry run black .
	poetry run pflake8 .
	poetry run mypy .

.PHONY: test
test:
	poetry run pytest --cov=./jquantsapi tests/
