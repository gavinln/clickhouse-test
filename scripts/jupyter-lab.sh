#!/bin/bash

set -Eeuo pipefail

# jupyter lab --port 8888 --ip=127.0.0.1 --no-browser --notebook-dir=/vagrant/notebooks

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

IP_ADDRESS=127.0.0.1

NET_INTERFACE=enp0s8
# IP_ADDRESS=$(ip -4 -o addr show $NET_INTERFACE | grep -Eo 'inet [0-9\.]+' | sed 's/inet //')

JUPYTER_CMD="jupyter lab --port=8888 --ip=$IP_ADDRESS --no-browser"

NOTEBOOKS=$SCRIPT_DIR/../notebooks

pipenv run $JUPYTER_CMD --notebook-dir=$NOTEBOOKS
