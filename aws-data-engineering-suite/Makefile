# AWS Data Engineering & Real-Time Analytics Suite Makefile
# Commands: make setup, make deploy-3.1, make deploy-3.2, make test, make destroy

SHELL := /bin/bash
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

.PHONY: help setup deploy-3.1 deploy-3.2 test destroy clean

help:
	@echo "Available commands:"
	@echo "  make setup       - Set up virtual environment and install requirements"
	@echo "  make deploy-3.1  - Deploy Project 3.1 (S3 Data Lake, Glue ETL, Athena)"
	@echo "  make deploy-3.2  - Deploy Project 3.2 (Kinesis Stream, Lambda, OpenSearch)"
	@echo "  make test        - Run automated pytest verification suite"
	@echo "  make destroy     - Execute zero-billing FinOps teardown"

setup:
	@echo "=== [Phase 1: Bootstrap] Initializing environment ==="
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Validating AWS CLI credentials (if configured)..."
	@if command -v aws >/dev/null 2>&1; then \
		aws sts get-caller-identity || echo "AWS CLI installed but no valid active session. Ensure ~/.aws/credentials or env vars are set."; \
	else \
		echo "AWS CLI not found on PATH. Ensure AWS CLI is installed for live cloud deployment."; \
	fi
	@echo "Verifying pytest discovery..."
	$(PYTEST) --collect-only scripts/test_data_pipelines.py

deploy-3.1:
	@echo "=== [Phase 2 & 3: Project 3.1 Serverless Data Lake & PySpark ETL] ==="
	@echo "Generating Bronze transactional data..."
	$(PYTHON) project_3_1_serverless_datalake/retail_data_generator.py
	@echo "Running Glue Bronze to Silver ETL..."
	$(PYTHON) project_3_1_serverless_datalake/glue_etl_bronze_to_silver.py
	@echo "Provisioning Glue Data Catalog and running Athena queries..."
	$(PYTHON) project_3_1_serverless_datalake/catalog_and_athena.py

deploy-3.2:
	@echo "=== [Phase 4 & 5: Project 3.2 Real-Time Streaming Analytics] ==="
	@echo "Configuring OpenSearch index schema and templates..."
	$(PYTHON) project_3_2_realtime_streaming/opensearch_index_setup.py
	@echo "Streaming IoT events and injecting temperature anomalies into Kinesis..."
	$(PYTHON) project_3_2_realtime_streaming/kinesis_iot_producer.py

test:
	@echo "=== [Phase 6: Verification] Running pytest suite ==="
	PYTHONPATH=. $(PYTEST) -v scripts/test_data_pipelines.py

destroy:
	@echo "=== [FinOps Teardown] Terminating streaming and cloud resources ==="
	$(PYTHON) scripts/cleanup_data_infra.py

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache project_3_1_serverless_datalake/__pycache__ project_3_2_realtime_streaming/__pycache__ scripts/__pycache__ *.csv

