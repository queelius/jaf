# Makefile for jaf project

.PHONY: help docs-serve docs-build docs-deploy clean install-dev

help:
	@echo "Available commands:"
	@echo "  install-dev    Install package in editable mode with dev dependencies"
	@echo "  docs-serve     Serve documentation locally"
	@echo "  docs-build     Build documentation"
	@echo "  docs-deploy    Deploy documentation to GitHub Pages"
	@echo "  clean          Remove build artifacts and __pycache__ directories"

install-dev:
	pip install -e .[dev]

docs-serve:
	mkdocs serve

docs-build:
	mkdocs build

docs-deploy:
	mkdocs gh-deploy --force

clean:
	rm -rf build dist *.egg-info site/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
