
# Help
.PHONY: help

help:
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# Local installation
.PHONY: init clean lock update install

install: ## Initalise the virtual env installing deps
	uv sync --all-extras

clean: ## Remove all the unwanted clutter
	find src -type d -name __pycache__ | xargs rm -rf
	find src -type d -name '*.egg-info' | xargs rm -rf
	rm -rf .venv

lock: ## Lock dependencies
	uv lock

update: ## Update dependencies (whole tree)
	uv lock --upgrade

sync: ## Install dependencies as per the lock file
	uv sync --all-extras

# Linting and formatting
.PHONY: lint test format

lint: ## Lint files with flake and mypy
	uv run flake8 src tests
	uv run mypy src tests
	uv run black --check src tests
	uv run isort --check-only src tests


format: ## Run black and isort
	uv run black src tests
	uv run isort src tests

# Testing

.PHONY: test tests unit

# Pytest test commands
# Run all tests
tests: ## Run all tests
	uv run pytest -s -v

# Run specific test file or test class
# Usage: make test TEST="logging/test_chat_logger.py"
#        make test TEST="logging/test_chat_logger.py::TestChatLoggerConnection"
test: ## Run a specific test expression (requires TEST=...)
	@if [ -z "$(TEST)" ]; then \
		echo "Error: TEST parameter is required. Usage: make test TEST='test_function_name'"; \
		exit 1; \
	fi
	uv run pytest -s -k $(TEST)

unit: ## Run unit tests only
	uv run pytest tests/unit

# Release
package:
	# create a source distribution
	uv run python -m build --sdist
	# create a wheel
	uv run python -m build --wheel

release: package
	uv run twine upload dist/*
