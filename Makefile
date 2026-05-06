.PHONY: up down restart ps logs topics stream stream-logs clean

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

clean:
	$(COMPOSE) down -v
