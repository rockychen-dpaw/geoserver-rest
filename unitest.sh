#!/bin/bash
set -a
source ./.env
set +a

poetry run python -m geoserver_rest.unitest.test_about
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_usergroup
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_reload
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_security
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_workspace
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_datastore
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_featuretype
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_style
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_wmsstore
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_wmslayer
if [[ $? != 0 ]]
then
    exit 1
fi

poetry run python -m geoserver_rest.unitest.test_gwc
if [[ $? != 0 ]]
then
    exit 1
fi
