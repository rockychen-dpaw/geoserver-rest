#!/bin/bash
set -a
source ./.env
set +a

uv run python -m geoserver_rest.gwcclean
if [[ $? != 0 ]]
then
    exit 1
fi
