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
	bash $(SCRIPT_DIR)/scripts/jupyter-nb.sh

.PHONY: jupyter-nb
jupyter-nb:  ## start jupyter notebook
	bash $(SCRIPT_DIR)/scripts/jupyter-lab.sh

.PHONY: clickhouse-start
clickhouse-start:  ## start Clickhouse
	@docker-compose -f clickhouse/docker-compose.yml up -d

.PHONY: clickhouse-stop
clickhouse-stop:  ## stop Clickhouse
	docker-compose -f clickhouse/docker-compose.yml stop

tmux:  ## start tmux
	tmuxp load tmux.yaml
