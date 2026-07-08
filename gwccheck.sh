#!/bin/bash
set -a
source ./.env
set +a

uv run python -m geoserver_rest.gwccheck
if [[ $? != 0 ]]
then
    exit 1
fi
