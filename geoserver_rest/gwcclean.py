import os
from datetime import datetime

from . import settings
from . import loggingconfig
from . import utils
from . import gwcmanage


if __name__ == '__main__':
    geoserver_name = os.environ["GEOSERVER_NAME"]
    geoserver_url = os.environ["GEOSERVER_URL"]
    geoserver_user = os.environ["GEOSERVER_USER"]
    geoserver_password = os.environ["GEOSERVER_PASSWORD"]
    geoserver_ssl_verify = os.environ.get("GEOSERVER_SSL_VERIFY","true").lower() == "true"
   
    if not geoserver_name:
        geoserver_name = utils.get_domain(geoserver_url)

    gwc_tiles_dir = os.environ.get("GWC_TILES_DIR")
    if not gwc_tiles_dir:
        raise Exception("Missing gwc_tiles_dir")
    gwc_disk_size = os.environ.get("GWC_DISK_SIZE")
    gwc_disk_shared = os.environ.get("GWC_DISK_SHARED","false").lower() == "true"
    #seconds
    gwc_cleantime = int(os.environ.get("GWC_CLEANTIME",0))
    

    gwcmanage = gwcmanage.GWCManage(geoserver_name,geoserver_url,geoserver_user,geoserver_password,geoserver_ssl_verify,gwc_tiles_dir,gwc_disk_size,gwc_disk_shared,settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"))
    gwcmanage.clean_gwc_cache(gwc_cleantime)


