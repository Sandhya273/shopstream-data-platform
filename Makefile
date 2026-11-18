.PHONY: install run test lint dbt-docs clean

install:
	pip install -r requirements-dev.txt

run:            ## full local pipeline
	python run_local.py

test:
	pytest -q tests

lint:
	ruff check ingestion data_quality data_generator scripts tests run_local.py bi

dbt-docs:
	cd dbt && DBT_PROFILES_DIR=. dbt docs generate && DBT_PROFILES_DIR=. dbt docs serve

clean:
	rm -rf landing warehouse dbt/target dbt/logs bi/dashboard_data.json
