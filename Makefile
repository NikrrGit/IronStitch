.PHONY: up down restart ps logs topics clean

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

clean:
	$(COMPOSE) down -v
