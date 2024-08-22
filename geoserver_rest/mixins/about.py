import logging

logger = logging.getLogger(__name__)

class AboutMixin(object):
    def version_url(self):
        return "{0}/rest/about/version".format(self.geoserver_url)
    
    VERSIONS = {}
    def get_version(self,component="geoserver"):
        if self.geoserver_url not in self.VERSIONS:
            res = self.get(self.version_url(),headers=self.accept_header("json"))
            data = {}
            for d in res.json().get("about",{}).get("resource") or []:
                if isinstance(d["Version"],int):
                    data[d["@name"].lower()] = [d["Version"]]
                elif isinstance(d["Version"],float):
                    data[d["@name"].lower()] = [int(i) for i in str(d["Version"]).split(".")]
                else:
                    data[d["@name"].lower()] = [int(i) for i in d["Version"].split(".")]

            self.VERSIONS[self.geoserver_url] = data
 
        if component:
            return self.VERSIONS[self.geoserver_url].get(component.lower()) or None
        else:
            return self.VERSIONS[self.geoserver_url]
    
