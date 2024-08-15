import logging.config

from . import settings

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
            'level': 'DEBUG' if settings.DEBUG else "INFO",
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
            'level': 'DEBUG' if settings.DEBUG else "INFO",
            'propagate': False
        }
    }
})

