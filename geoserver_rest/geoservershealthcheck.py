import os
import logging
import traceback

from . import settings
from . import utils
from . import loggingconfig
from .geoserverhealthcheck import GeoserverHealthCheck

logger = logging.getLogger("geoserver_rest.geoservershealthcheck")

class GeoserversHealthCheck(object):
    
    def __init__(self,geoservers,requestheaders=None,dop=1):
        self.healthchecks = [GeoserverHealthCheck(geoserver[0],geoserver[1],geoserver[2],geoserver[3],requestheaders=requestheaders,dop=dop) for geoserver in geoservers]

    def start(self):
        for healthcheck in self.healthchecks:
            healthcheck.start()

    def wait_to_finish(self):
        for healthcheck in self.healthchecks:
            try:
                healthcheck.wait_to_finish()
            except:
                logger.error(traceback.format_exc())


if __name__ == '__main__':
    parse_url = lambda data: [ d.strip() for d in data.rsplit("=",1)] if "=" in data else (utils.get_domain(data),data)
    geoserver_urls = os.environ["GEOSERVER_URLS"]
    geoserver_urls = [ parse_url(u.strip()) for u in geoserver_urls.split(",") if u.strip()]
    geoserver_users = os.environ.get("GEOSERVER_USERS") or os.environ.get("GEOSERVER_USER") 
    geoserver_users = [u.strip() for u in geoserver_users.split(",") if u.strip()]
    if len(geoserver_users) == 1:
        geoserver_users = geoserver_users * len(geoserver_urls)
    elif len(geoserver_users) != len(geoserver_urls):
        raise Exception("The count({1}) of geoserver users does not match with the count({0}) of geoserver urls.".format(len(geoserver_urls),len(geoserver_users)))
    
    geoserver_passwords = os.environ.get("GEOSERVER_PASSWORDS") or os.environ.get("GEOSERVER_PASSWORD")
    geoserver_passwords = [u.strip() for u in geoserver_passwords.split(",") if u.strip()]
    if len(geoserver_passwords) == 1:
        geoserver_passwords = geoserver_passwords * len(geoserver_urls)
    elif len(geoserver_passwords) != len(geoserver_urls):
        raise Exception("The count({1}) of geoserver passwords does not match with the count({0}) of geoserver urls.".format(len(geoserver_urls),len(geoserver_passwords)))

    geoservers = [(geoserver_urls[i][0],geoserver_urls[i][1],geoserver_users[i],geoserver_passwords[i]) for i in range(len(geoserver_urls))]

    healthcheck = GeoserversHealthCheck(geoservers,settings.REQUEST_HEADERS,settings.HEALTHCHECK_DOP)
    healthcheck.start()
    healthcheck.wait_to_finish()


