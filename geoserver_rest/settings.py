import os
import logging.config



BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGGING_CONF = os.environ.get("LOGGING_CONF",os.path.join(BASE_DIR,"logging.conf"))
logging.config.fileConfig(LOGGING_CONF)

