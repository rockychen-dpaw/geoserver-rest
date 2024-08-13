import logging

logger = logging.getLogger(__name__)

class ReloadMixin(object):
    def reload_url(self):
        return "{0}/rest/reload".format(self.geoserver_url)
    
    def reload(self):
        res = self.put(self.reload_url(),None)
        logger.debug("Succeed to reload the geoserver catalogue.")
    
