VERSION := 1.0.0

export PYTHONPATH=$(shell pwd)
export PYTHONDONTWRITEBYTECODE=1

test:
	@py.test -x --cov=src/ --cov-report=term-missing -W ignore::DeprecationWarning

test-coverage: ## Run entire test suite with coverage
	@py.test -x src/ --cov=src/ --cov-report=term-missing --cov-report=xml --cov-fail-under=100

install-dependencies: ## Install requirements
	@pip install -U -r requirements.txt

run-etl:
	@python ./src/script/run.py
