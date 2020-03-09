ifeq ($(OS),Windows_NT)
    SHELL='c:/Program Files/Git/usr/bin/sh.exe'
endif

SCRIPT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.PHONY: help connect jupyter-nb
.DEFAULT_GOAL=help
help:  ## help for this Makefile
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

connect:  ## connect to gavinsvr 
	bash $(SCRIPT_DIR)/scripts/connect.sh

jupyter-nb:  ## start jupyter notebook
	bash $(SCRIPT_DIR)/scripts/jupyter-nb.sh

tmux:  ## start tmux
	tmuxp load tmux.yaml
