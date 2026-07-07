.PHONY: help lint test clean setup docker-build docker-test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Create conda environment
	conda env create -f environment.yml

setup-pip: ## Install via pip
	pip install -r requirements.txt

lint: ## Run Ruff + mypy
	ruff check src/ tests/
	mypy src/ --strict --ignore-missing-imports

test: ## Run unit tests
	pytest tests/ -v --tb=short

test-integration: ## Run full pipeline integration test
	PYSPARK_PYTHON=python3 python -c "
	import yaml, tempfile
	from pyspark.sql import SparkSession
	spark = SparkSession.builder.master('local[2]').appName('local-test').config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate()
	from src.silver import SilverTransformer
	from src.gold import GoldAggregator
	from src.quality import DataQuality
	df = spark.createDataFrame([('2024-06-01T00:00:00',-8.5,115.2,4.5,20.0,'Bali'),('2024-06-15T00:00:00',-7.3,109.8,3.2,10.0,'Java')],['time','latitude','longitude','mag','depth','place'])
	st = SilverTransformer('/tmp/bronze','/tmp/silver')
	clean = st.transform(df)
	st.load(clean)
	dq = DataQuality(spark)
	report = dq.report_all('silver')
	print('Integration: ✅', report.count(), 'checks')
	"

clean: ## Clean up temp files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete
	rm -rf .pytest_cache .mypy_cache

docker-build: ## Build Docker image
	docker build -t earthquake-etl .

docker-test: docker-build ## Run tests in Docker
	docker run --rm earthquake-etl

validate: lint test ## Run all checks before commit
	@echo '✅ All checks passed'
