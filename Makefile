# Makefile for Impala Transfer Tool
.PHONY: test lint format check clean install dev-install

# Python interpreter
PYTHON = python3
PIP = pip3

# Directories
SRC_DIR = impala_transfer
TEST_DIR = tests

# Install the package in development mode
install:
	$(PIP) install -e .

# Install development dependencies
dev-install:
	$(PIP) install -e .[dev]
	$(PIP) install black flake8 mypy pytest pytest-cov

# Run tests with coverage
test:
	pytest $(TEST_DIR)/ --cov=$(SRC_DIR) --cov-report=html --cov-report=term-missing

# Run linting
lint:
	flake8 $(SRC_DIR)/ $(TEST_DIR)/
	mypy $(SRC_DIR)/

# Format code
format:
	black $(SRC_DIR)/ $(TEST_DIR)/
	isort $(SRC_DIR)/ $(TEST_DIR)/

# Run all checks (lint + test)
check: lint test

# Clean up generated files
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .coverage htmlcov/
	rm -rf build/ dist/ *.egg-info/
	rm -rf temp/ tmp/
	rm -f *.log

# Install pre-commit hooks
install-hooks:
	pre-commit install

# Run pre-commit on all files
pre-commit:
	pre-commit run --all-files

# Build package
build:
	$(PYTHON) setup.py sdist bdist_wheel

# Install from requirements
install-deps:
	$(PIP) install -r requirements.txt

# Show help
help:
	@echo "Available commands:"
	@echo "  install       - Install package in development mode"
	@echo "  dev-install   - Install development dependencies"
	@echo "  test          - Run tests with coverage"
	@echo "  lint          - Run linting (flake8 + mypy)"
	@echo "  format        - Format code (black + isort)"
	@echo "  check         - Run all checks (lint + test)"
	@echo "  clean         - Clean up generated files"
	@echo "  install-hooks - Install pre-commit hooks"
	@echo "  pre-commit    - Run pre-commit on all files"
	@echo "  build         - Build package"
	@echo "  install-deps  - Install from requirements.txt"
	@echo "  help          - Show this help message" 