#!/bin/bash
if [[ "${GEOSERVER_URLS}" == "" ]]; then
    cd /app && python -m geoserver_rest.geoserverhealthcheck
else
    cd /app && python -m geoserver_rest.geoservershealthcheck
fi
