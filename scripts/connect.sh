#!/bin/bash

set -Eeuxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ssh -p 443 -t gavinsvr "cd ~/ws/clickhouse-test;/bin/bash"
