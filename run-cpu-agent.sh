#!/usr/bin/env bash

set -a
source .env
set +a

prefect agent start --pool "cpu-agents" --limit 4