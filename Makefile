ifeq ($(OS),Windows_NT)
    SHELL='c:/Program Files/Git/usr/bin/sh.exe'
endif

SCRIPT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.PHONY: help
.DEFAULT_GOAL=help
help:  ## help for this Makefile
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: connect 
connect:  ## connect to gavinsvr 
	bash $(SCRIPT_DIR)/scripts/connect.sh

.PHONY: jupyter-lab
jupyter-lab:  ## start jupyter notebook
	bash $(SCRIPT_DIR)/scripts/jupyter-lab.sh

.PHONY: ch-start
ch-start:  ## start Clickhouse
	@docker-compose -f clickhouse/docker-compose.yml up -d

.PHONY: ch-stop
ch-stop:  ## stop Clickhouse
	docker-compose -f clickhouse/docker-compose.yml stop

.PHONY: tmux
tmux:  ## start tmux
	tmuxp load tmux.yaml

.PHONY: ch-client
ch-client:  ## start clickhouse client
	clickhouse-client -m --highlight 0 --output_format_pretty_color 0

.PHONY: ruff
ruff:  ## run ruff Python style checker
	@test -n "$(fl)" || { echo "fl= not specified"; exit 1; }
	@test -f "$(fl)" || { echo "file $(fl) does not exist"; exit 1; }
	ruff --select F,E,D --ignore D203,D212 $(fl)
