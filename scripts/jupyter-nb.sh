#!/bin/bash

# jupyter notebook --port 8888 --ip=127.0.0.1 --no-browser --notebook-dir=/vagrant/notebooks

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

IP=127.0.0.1

JUPYTER_CMD="jupyter notebook --port=8888 --ip=$IP --no-browser"

NOTEBOOKS=$SCRIPT_DIR/../notebooks

pipenv run $JUPYTER_CMD --notebook-dir=$NOTEBOOKS
