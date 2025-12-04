#!/usr/bin/env bash

set -a
source .env
set +a
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH

prefect agent start --pool "gpu-agents" --limit 1