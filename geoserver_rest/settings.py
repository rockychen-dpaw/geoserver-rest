import os
import logging.config


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEBUG = os.environ.get("DEBUG","false").lower() == "true"

MAX_BBOX = [108,-45,155,-10]
GWC_GRIDSETS = os.environ.get("GWC_GRIDSETS","gda94").split(",")
GWC_GRIDSET = GWC_GRIDSETS[0]
MAP_FORMAT = os.environ.get("MAP_FORMAT","image/jpeg")
WMTS_VERSION = os.environ.get("WMTS_VERSION","1.0.0")
MAX_EXPIRE_CLIENTS = os.environ.get("MAX_EXPIRE_CLIENTS",86400 * 14)
MAX_EXPIRE_CLIENTS_STR = None
expires = MAX_EXPIRE_CLIENTS
if expires > 86400:
    days = int(expires / 86400)
    expires = expires % 86400
    if days == 1:
        MAX_EXPIRE_CLIENTS_STR = "1 Day"
    else:
        MAX_EXPIRE_CLIENTS_STR = "{} Days".format(days)

if expires > 3600:
    hours = int(expires / 3600)
    expires = expires % 3600
    if hours == 1:
        MAX_EXPIRE_CLIENTS_STR = "{}1 Hour".format("{} ".format(MAX_EXPIRE_CLIENTS_STR) if MAX_EXPIRE_CLIENTS_STR else "")
    else:
        MAX_EXPIRE_CLIENTS_STR = "{}{} Hours".format("{} ".format(MAX_EXPIRE_CLIENTS_STR) if MAX_EXPIRE_CLIENTS_STR else "",hours)

minutes = int(expires / 60)
if minutes == 1:
    MAX_EXPIRE_CLIENTS_STR = "{}1 Minute".format("{} ".format(MAX_EXPIRE_CLIENTS_STR) if MAX_EXPIRE_CLIENTS_STR else "")
elif minutes > 1:
    MAX_EXPIRE_CLIENTS_STR = "{}{} Minutes".format("{} ".format(MAX_EXPIRE_CLIENTS_STR) if MAX_EXPIRE_CLIENTS_STR else "",minutes)


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else "INFO",
            'formatter': 'console',
            'class': 'logging.StreamHandler',
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False
        },
        'geoserver_rest': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else "INFO",
            'propagate': False
        }
    }
})

