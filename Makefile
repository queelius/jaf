# Makefile for JAF project
# Self-contained development environment using Python venv

# Project configuration
PROJECT_NAME = jaf
PYTHON = python3
VENV_DIR = venv
VENV_BIN = $(VENV_DIR)/bin
PYTHON_VENV = $(VENV_BIN)/python
PIP_VENV = $(VENV_BIN)/pip

# Get version from pyproject.toml
VERSION = $(shell grep '^version = ' pyproject.toml | sed 's/.*"\(.*\)".*/\1/')

.PHONY: help venv install install-dev clean test test-cov lint format type-check \
        docs-serve docs-build docs-deploy build dist upload tag-release \
        check-git-clean bump-patch bump-minor bump-major release

help: ## Show this help message
	@echo "JAF (JSON Array Filter) - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  venv           Create Python virtual environment"
	@echo "  install        Install package in development mode"
	@echo "  install-dev    Install with development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  test           Run all tests"
	@echo "  test-cov       Run tests with coverage report"
	@echo "  lint           Run flake8 linting"
	@echo "  format         Format code with black (if available)"
	@echo "  type-check     Run mypy type checking (if available)"
	@echo "  clean          Remove build artifacts and caches"
	@echo ""
	@echo "Documentation:"
	@echo "  docs-serve     Serve documentation locally"
	@echo "  docs-build     Build documentation"
	@echo "  docs-deploy    Deploy documentation to GitHub Pages"
	@echo ""
	@echo "Release Management:"
	@echo "  build          Build distribution packages"
	@echo "  dist           Create source and wheel distributions"
	@echo "  upload         Upload to PyPI (requires authentication)"
	@echo "  bump-patch     Bump patch version (0.5.2 -> 0.5.3)"
	@echo "  bump-minor     Bump minor version (0.5.2 -> 0.6.0)"
	@echo "  bump-major     Bump major version (0.5.2 -> 1.0.0)"
	@echo "  tag-release    Create and push git tag for current version"
	@echo "  release        Full release process: test, build, tag, upload"
	@echo ""
	@echo "Current version: $(VERSION)"

# Virtual environment setup
venv: ## Create Python virtual environment
	@if [ ! -d $(VENV_DIR) ]; then \
		$(PYTHON) -m venv $(VENV_DIR); \
		$(PIP_VENV) install --upgrade pip setuptools wheel; \
	fi

# Installation targets
install: venv ## Install package in development mode
	$(PIP_VENV) install -e .

install-dev: venv ## Install with development dependencies
	$(PIP_VENV) install -e .[dev]

# Development targets
test: venv ## Run all tests
	$(VENV_BIN)/pytest 

test-cov: venv ## Run tests with coverage report
	$(VENV_BIN)/pytest --cov=$(PROJECT_NAME) --cov-report=html --cov-report=term

lint: venv ## Run flake8 linting
	$(VENV_BIN)/flake8 $(PROJECT_NAME)/ tests/ || echo "flake8 not available, install with: pip install flake8"

format: venv ## Format code with black (optional)
	@if $(VENV_BIN)/python -c "import black" 2>/dev/null; then \
		$(VENV_BIN)/black $(PROJECT_NAME)/ tests/; \
	else \
		echo "black not installed. Install with: pip install black"; \
	fi

type-check: venv ## Run mypy type checking (optional)
	@if $(VENV_BIN)/python -c "import mypy" 2>/dev/null; then \
		$(VENV_BIN)/mypy $(PROJECT_NAME)/; \
	else \
		echo "mypy not installed. Install with: pip install mypy"; \
	fi

# Documentation targets
docs-serve: venv ## Serve documentation locally
	$(VENV_BIN)/mkdocs serve

docs-build: venv ## Build documentation
	$(VENV_BIN)/mkdocs build

docs-deploy: venv ## Deploy documentation to GitHub Pages
	$(VENV_BIN)/mkdocs gh-deploy --force

# Build and distribution
build: venv ## Build distribution packages
	$(PYTHON_VENV) -m build

dist: clean build ## Create source and wheel distributions
	@echo "Distribution packages created:"
	@ls -la dist/

# Version management
bump-patch: ## Bump patch version (0.5.2 -> 0.5.3)
	@current_version=$(VERSION); \
	new_version=$$(echo $$current_version | awk -F. '{$$3++; print $$1"."$$2"."$$3}'); \
	sed -i 's/version = "$(VERSION)"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i 's/__version__ = "$(VERSION)"/__version__ = "'$$new_version'"/' $(PROJECT_NAME)/__init__.py; \
	echo "Version bumped from $(VERSION) to $$new_version"

bump-minor: ## Bump minor version (0.5.2 -> 0.6.0)
	@current_version=$(VERSION); \
	new_version=$$(echo $$current_version | awk -F. '{$$2++; $$3=0; print $$1"."$$2"."$$3}'); \
	sed -i 's/version = "$(VERSION)"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i 's/__version__ = "$(VERSION)"/__version__ = "'$$new_version'"/' $(PROJECT_NAME)/__init__.py; \
	echo "Version bumped from $(VERSION) to $$new_version"

bump-major: ## Bump major version (0.5.2 -> 1.0.0)
	@current_version=$(VERSION); \
	new_version=$$(echo $$current_version | awk -F. '{$$1++; $$2=0; $$3=0; print $$1"."$$2"."$$3}'); \
	sed -i 's/version = "$(VERSION)"/version = "'$$new_version'"/' pyproject.toml; \
	sed -i 's/__version__ = "$(VERSION)"/__version__ = "'$$new_version'"/' $(PROJECT_NAME)/__init__.py; \
	echo "Version bumped from $(VERSION) to $$new_version"

# Git and release management
check-git-clean: ## Check if git working directory is clean
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Error: Git working directory is not clean. Commit or stash changes first."; \
		git status --short; \
		exit 1; \
	fi

tag-release: check-git-clean ## Create and push git tag for current version
	@echo "Creating git tag v$(VERSION)..."
	git tag -a v$(VERSION) -m "Release version $(VERSION)"
	git push origin v$(VERSION)
	@echo "Tag v$(VERSION) created and pushed"

upload: dist ## Upload to PyPI (requires authentication)
	@echo "Uploading version $(VERSION) to PyPI..."
	@if $(VENV_BIN)/python -c "import twine" 2>/dev/null; then \
		$(VENV_BIN)/twine upload dist/*; \
	else \
		echo "Error: twine not installed. Install with: pip install twine"; \
		exit 1; \
	fi

release: ## Full release process: test, build, tag, upload
	@echo "Starting release process for version $(VERSION)..."
	$(MAKE) check-git-clean
	$(MAKE) test
	$(MAKE) lint
	$(MAKE) clean
	$(MAKE) dist
	$(MAKE) tag-release
	$(MAKE) upload
	@echo "Release $(VERSION) completed successfully!"

# Cleanup
clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info/ site/
	rm -rf htmlcov/ .coverage .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# Clean everything including venv
clean-all: clean ## Remove everything including virtual environment
	rm -rf $(VENV_DIR)

# Development shortcuts
dev: install-dev ## Setup complete development environment
	@echo "Development environment ready!"
	@echo "Activate with: source $(VENV_BIN)/activate"

check: test lint ## Run all checks (tests + linting)

# Show current project status
status: ## Show project status and version info
	@echo "Project: $(PROJECT_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Python: $$($(PYTHON) --version 2>&1)"
	@echo "Venv: $$(if [ -d $(VENV_DIR) ]; then echo 'Created'; else echo 'Not created'; fi)"
	@echo "Git status:"
	@git status --short || echo "Not a git repository"