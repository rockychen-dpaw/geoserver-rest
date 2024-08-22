from .workspacetasks import *
from .datastoretasks import *
from .featuretypetasks import *
from .wmsstoretasks import *
from .wmslayertasks import *
from .layergrouptasks import *
from .mapservicetasks import *


class CheckGeoserverAlive(Task):
    """
    Return geoserver version
    Test whether geoserver is accessible
    """
    category = "Check Geoserver Alive"
    def __init__(self,post_actions_factory = None):
        super().__init__(post_actions_factory = post_actions_factory)

    def _format_result(self):
        return "Geoserver is alive.\r\n{}".format(json.dumps(self.result,indent=4) if self.result else "")

    def _exec(self,geoserver):
        return geoserver.get_version(None)
    
