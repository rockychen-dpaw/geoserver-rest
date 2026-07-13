import os
from datetime import datetime

from . import settings
from . import loggingconfig
from . import utils
from . import gwcmanager


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
    #seconds
    max_cleantime = int(os.environ.get("GWC_MAX_CLEANTIME",0))
    clean_interval = int(os.environ.get("GWC_CLEAN_INTERVAL",10)) #days
    check_increments = int(os.environ.get("GWC_CHECK_INCREMENTS",5)) * 1048576 #G
    clean_threshold = float(os.environ.get("GWC_CLEAN_THRESHOLD",0.8))
    emergencyclean_threshold = float(os.environ.get("GWC_EMERGENCYCLEAN_THRESHOLD",0.95))
    

    gwcmanager = gwcmanager.GWCManager(
        geoserver_name,
        geoserver_url,
        geoserver_user,
        geoserver_password,
        geoserver_ssl_verify,
        gwc_tiles_dir,
        gwc_disk_size,
        settings.GET_REQUEST_HEADERS("GEOSERVER_REQUEST_HEADERS"))
    clean_result,check_result = gwcmanager.manage(
        clean_interval=clean_interval,
        check_increments=check_increments,
        max_cleantime=max_cleantime,
        clean_threshold=clean_threshold,
        emergencyclean_threshold=emergencyclean_threshold
    )
    if clean_result[0]:
        if clean_result[1] < 0:
            print("Performed a tile cleaning task, but can't find how much disk space was released.")
        elif clean_result[1] <= 1024:
            print("Performed a tile cleaning task,{}K space was released".format(clean_result[1]))
        elif clean_result[1] <= 1048576:
            print("Performed a tile cleaning task,{}M space was released".format(clean_result[1] / 1024))
        else:
            print("Performed a tile cleaning task,{}G space was released".format(clean_result[1] / 1048576))
    else:
        print("Tile cleaning task was skipped.")

    if check_result[0]:
        if not check_result[2]:
            print("Performed a GWC layers' cache detecting task, but can't find how much disk space the gwc layers used.")
        elif check_result[2] <= 1024:
            print("Performed a GWC layers' cache detecting task, {}K space was occupied by gwc layers, Used {}%".format(check_result[2],check_result[3] * 100))
        elif check_result[2] <= 1048576:
            print("Performed a GWC layers' cache detecting task, {}M space was occupied by gwc layers, Used {}%".format(check_result[2] / 1024,check_result[3] * 100))
        else:
            print("Performed a GWC layers' cache detecting task, {}G space was occupied by gwc layers, Used {}%".format(check_result[2] / 1048576,check_result[3] * 100))
    else:
        print("GWC layers' cache detecting task was skipped.")
        


