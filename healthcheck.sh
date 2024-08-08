#!/bin/bash
set -a
source ./.env
set +a

poetry run python -m geoserver_rest.healthcheck
if [[ $? != 0 ]]
then
    exit 1
fi
