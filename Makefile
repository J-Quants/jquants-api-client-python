
.PHONY: format
format:
	poetry run isort .
	poetry run black .
	poetry run pflake8 .
	poetry run mypy .

.PHONY: test
test:
	poetry run pytest
