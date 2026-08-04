.PHONY: setup teardown dbt-run register-cdc

# Spin up all infrastructure
setup:
	@echo "Starting Docker Compose..."
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@python -c "import time; time.sleep(10)"
	@echo "Registering Debezium CDC Connector..."
	./ingestion/debezium/register_connector.sh

# Run dbt transformations
dbt-run:
	@echo "Running dbt models..."
	cd processing/dbt_pricing && dbt run --profiles-dir .

# Teardown infrastructure
teardown:
	@echo "Stopping and removing containers..."
	docker-compose down -v