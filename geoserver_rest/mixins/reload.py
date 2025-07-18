import logging

logger = logging.getLogger(__name__)

class ReloadMixin(object):
    def reload_url(self):
        return "{0}/rest/reload".format(self.geoserver_url)
    
    def reset_url(self):
        return "{0}/rest/reset".format(self.geoserver_url)
    
    def reload(self):
        res = self.put(self.reload_url(),None,timeout=None)
        logger.debug("Succeed to reload the geoserver catalogue.")
    
    def reset(self):
        res = self.put(self.reset_url(),None,timeout=None)
        logger.debug("Succeed to reset the geoserver's authentication,store, raster and schema caches'.")
    
