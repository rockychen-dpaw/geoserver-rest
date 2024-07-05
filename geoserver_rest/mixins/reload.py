import logging

logger = logging.getLogger(__name__)

class ReloadMixin(object):
    def reload_url(self):
        return "{0}/rest/reload".format(self.geoserver_url)
    
    def reload(self):
        r = self.put(self.reload_url(),None)
        if r.status_code >= 300:
            raise Exception("Failed to reload geoserver catalogue. code = {} , message = {}".format(r.status_code, r.content))
        else:
            logger.debug("Succeed to reload the geoserver catalogue.")
    
