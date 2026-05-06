.PHONY: up down restart ps logs topics stream stream-logs wait wait-spark stack-ready smoke smoke-reset clean

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

wait-spark:
	@echo "Waiting for Spark streaming query to start (up to 180s)..."
	@for i in $$(seq 1 90); do \
		if docker logs ironstitch-spark 2>&1 | grep -q "Streaming query made progress\|Initializing sources"; then \
			echo "Spark streaming is alive"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "Spark streaming did not start in time. Check 'make stream-logs'"; \
	exit 1

stack-ready: up wait wait-spark
	@$(COMPOSE) ps

smoke: stack-ready topics
	@echo "Preparing smoke input CSV (first 25 rows)..."
	@[ -f data/olist_order_items_dataset.csv ] || (echo "Missing source CSV: data/olist_order_items_dataset.csv" && exit 1)
	@head -n 26 data/olist_order_items_dataset.csv > data/smoke_order_items.csv
	@echo "Wrote data/smoke_order_items.csv"
	@echo "Producing smoke events..."
	@csv_path=data/smoke_order_items.csv python3 producer/src/main.py
	@echo "Polling DuckDB for rows (up to 120s)..."
	@COUNT=0; \
	for i in $$(seq 1 60); do \
		if [ -f data/duckdb/orders.duckdb ]; then \
			COUNT=$$($(COMPOSE) exec -T spark python3 -c "import duckdb; con=duckdb.connect('/opt/ironstitch/data/duckdb/orders.duckdb'); t=con.execute(\"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='orders'\").fetchone()[0]; print(con.execute('SELECT COUNT(*) FROM orders').fetchone()[0] if t else 0)" 2>/dev/null | tr -d '[:space:]'); \
		fi; \
		if [ -n "$$COUNT" ] && [ "$$COUNT" -gt 0 ]; then \
			echo "DuckDB row count: $$COUNT"; \
			break; \
		fi; \
		sleep 2; \
	done; \
	if [ -z "$$COUNT" ] || [ "$$COUNT" -eq 0 ]; then \
		echo "DuckDB still empty after waiting. Check 'make stream-logs'."; \
		exit 1; \
	fi
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
