
format:
	@ruff format

lint:
	@ruff .
	@mypy


test:
	@pytest --cov . distiller/ tests/

