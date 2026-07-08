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
    gwc_disk_size = os.environ.get("GWC_DISK_SIZE")

    gwc_emergencyclean_threadhold = float(os.environ.get("GWC_EMERGENCYCLEAN_THREADHOLD",0.9))
    if gwc_emergencyclean_threadhold <= 0.5 or gwc_emergencyclean_threadhold >= 0.98:
        gwc_emergencyclean_threadhold = 0.9
    #seconds
    

    gwcmanage = gwcmanage.GWCManage(geoserver_name,geoserver_url,geoserver_user,geoserver_password,geoserver_ssl_verify,gwc_tiles_dir,gwc_disk_size,settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"))
    cleaning_rounds = gwcmanage.check_gwc_cache(gwc_emergencyclean_threadhold)
    if cleaning_rounds == 1:
        print("Succeed analysed the disk usage of gwc layers and perform 1 round of emergency cleaning.")
    elif cleaning_rounds > 1:
        print("Succeed analysed the disk usage of gwc layers and perform {} rounds of emergency cleaning.".format(cleaning_rounds))
    else:
        print("Successfully analysed the disk usage of gwc layers.")


