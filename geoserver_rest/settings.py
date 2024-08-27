import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEBUG = os.environ.get("DEBUG","false").lower() == "true"
DEFAULT_CHARSET = "utf-8"

EMAIL_USE_LOCALTIME=True

EMAIL_HOST = os.environ.get("EMAIL_HOST")
EMAIL_USE_TLS = os.environ.get("EMAIL_USE_TLS","false").lower() == "true"
EMAIL_USE_SSL = os.environ.get("EMAIL_USE_SSL","false").lower() == "true"
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587 if EMAIL_USE_TLS else (465 if EMAIL_USE_SSL else 25)))
EMAIL_SSL_CERTFILE = os.environ.get("EMAIL_SSL_CERTFILE")
EMAIL_SSL_KEYFILE = os.environ.get("EMAIL_SSL_KEYFILE")
EMAIL_HOST_USER = os.environ.get("EMAIL_HOST_USER") or ""
EMAIL_HOST_PASSWORD = os.environ.get("EMAIL_HOST_PASSWORD") or ""
EMAIL_TIMEOUT = os.environ.get("EMAIL_TIMEOUT")
EMAIL_TIMEOUT = int(EMAIL_TIMEOUT) if EMAIL_TIMEOUT is not None else None
EMAIL_FROM = os.environ.get("EMAIL_FROM","noreply@dbca.wa.gov.au")
EMAIL_TO = os.environ.get("EMAIL_TO")
EMAIL_TO = [e.strip() for e in EMAIL_TO.split(",") if e.strip()] if EMAIL_TO else None
EMAIL_CC = os.environ.get("EMAIL_CC")
EMAIL_CC = [e.strip() for e in EMAIL_CC.split(",") if e.strip()] if EMAIL_CC else None
EMAIL_BCC = os.environ.get("EMAIL_BCC")
EMAIL_BCC = [e.strip() for e in EMAIL_BCC.split(",") if e.strip()] if EMAIL_BCC else None
EMAIL_ENABLED = True if (EMAIL_HOST and (EMAIL_TO or EMAIL_CC or EMAIL_BCC)) else False

MAX_BBOX = [108,-45,155,-10]
GWC_GRIDSETS = os.environ.get("GWC_GRIDSETS","gda94").split(",")
GWC_GRIDSET = GWC_GRIDSETS[0]
TEST_FORMAT = os.environ.get("TEST_FORMAT","image/jpeg")
WMTS_VERSION = os.environ.get("WMTS_VERSION","1.0.0")
TEST_ZOOM = int(os.environ.get("TEST_ZOOM",12))
TEST_FEATURES_COUNT = int(os.environ.get("TEST_FEATURES_COUNT",1))

GEOSERVER_RESTART_TIMEOUT = int(os.environ.get("GEOSERVER_RESTART_TIMEOUT",1800))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT",5))
WMS_TIMEOUT = int(os.environ.get("WMS_TIMEOUT",600))
WMTS_TIMEOUT = int(os.environ.get("WMTS_TIMEOUT",600))
GETFEATURE_TIMEOUT = int(os.environ.get("GETFEATURE_TIMEOUT",600))
GETCAPABILITY_TIMEOUT = int(os.environ.get("GETCAPABILITY_TIMEOUT",600))

REQUEST_HEADERS = os.environ.get("REQUEST_HEADERS")
if REQUEST_HEADERS:
    REQUEST_HEADERS = dict([(header.strip().split("=",1) if "=" in header else [header,""]) for header in REQUEST_HEADERS.split(",") if header.strip()])

HEALTHCHECK_DOP = int(os.environ.get("HEALTHCHECK_DOP",2))

IGNORE_EMPTY_WORKSPACE = os.environ.get("IGNORE_EMPTY_WORKSPACE","false").lower() == "true"
IGNORE_EMPTY_DATASTORE = os.environ.get("IGNORE_EMPTY_DATASTORE","false").lower() == "true"
IGNORE_EMPTY_WMSSTORE = os.environ.get("IGNORE_EMPTY_WMSSTORE","false").lower() == "true"

REPORT_HOME = os.environ.get("REPORT_HOME") or "./reports"
if os.path.exists(REPORT_HOME):
    if not os.path.isdir(REPORT_HOME):
        raise Exception("Report home({}) is not a directory.".format(REPORT_HOME))
else:
    os.makedirs(REPORT_HOME,mode=0o755)

MAX_REPORTS = int(os.environ.get("MAX_REPORTS",30))

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

