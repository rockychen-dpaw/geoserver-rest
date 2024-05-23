import logging

logger = logging.getLogger(__name__)

class AboutMixin(object):
    def version_url(self):
        return "{0}/rest/about/version".format(self.geoserver_url)
    
    VERSIONS = {}
    def get_version(self,component="geoserver"):
        if self.geoserver_url not in self.VERSIONS:
            r = self.get(self.version_url(),headers=self.accept_header("json"))
            if r >= 300:
                r.raise_for_status()
            data = {}
            for d in r.json().get("about",{}),get("resource") or []:
                data[d["@name"].lower()] = [int(i) for i in d["Version"].split(".")]

            self.VERSIONS[self.geoserver_url] = data
                
        return self.VERSIONS[self.geoserver_url].get(component.lower()) or None
    
