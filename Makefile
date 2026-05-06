.PHONY: up down restart ps logs topics stream stream-logs wait stack-ready smoke smoke-reset clean

COMPOSE := docker compose
REDPANDA := $(COMPOSE) exec redpanda

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topics:
	$(REDPANDA) rpk topic create orders.v1 orders.dlq -p 3 || true
	$(REDPANDA) rpk topic list

stream:
	$(COMPOSE) up -d spark

stream-logs:
	$(COMPOSE) logs -f spark --tail=200

wait:
	@echo "Waiting for Redpanda health..."
	@for i in $$(seq 1 60); do \
		STATUS=$$(docker inspect -f '{{.State.Health.Status}}' ironstitch-redpanda 2>/dev/null || true); \
		if [ "$$STATUS" = "healthy" ]; then \
			echo "Redpanda is healthy"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "Redpanda did not become healthy in time"; \
	exit 1

stack-ready: up wait
	@echo "Giving Spark extra startup time (dependencies + jars)..."
	@sleep 15
	@$(COMPOSE) ps

smoke: stack-ready topics
	@echo "Preparing smoke input CSV (first 25 rows)..."
	@[ -f data/olist_order_items_dataset.csv ] || (echo "Missing source CSV: data/olist_order_items_dataset.csv" && exit 1)
	@head -n 26 data/olist_order_items_dataset.csv > data/smoke_order_items.csv
	@echo "Wrote data/smoke_order_items.csv"
	@echo "Producing smoke events..."
	@csv_path=data/smoke_order_items.csv python3 producer/src/main.py
	@echo "Waiting for Spark to process batches..."
	@sleep 20
	@echo "DuckDB row count:"
	@$(COMPOSE) exec spark python -c "import duckdb; print(duckdb.connect('/opt/ironstitch/data/duckdb/orders.duckdb').execute('SELECT COUNT(*) FROM orders').fetchall())"
	@echo "DLQ topic offsets:"
	@$(REDPANDA) rpk topic describe orders.dlq | sed -n '1,120p'
	@echo "Smoke run complete."

smoke-reset:
	@echo "Resetting checkpoints + local DuckDB state..."
	@rm -rf data/checkpoints data/duckdb data/smoke_order_items.csv
	@mkdir -p data/checkpoints data/duckdb
	@$(COMPOSE) restart spark
	@echo "Reset complete."

clean:
	$(COMPOSE) down -v
