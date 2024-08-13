import logging
import os

logger = logging.getLogger(__name__)

CATALOGUE_MODE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <mode>{}</mode>
</catalog>
"""
RULES_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<rules>
    {}
</rules>
"""
RULE_TEMPLATE = """<rule resource="{0}">{1}</rule>"""
class SecurityMixin(object):
    def catalogue_mode_url(self):
        return "{0}/rest/security/acl/catalog".format(self.geoserver_url)
    
    def layer_access_rules_url(self):
        return "{0}/rest/security/acl/layers".format(self.geoserver_url)
    
    def layer_access_rule_url(self,layerrule):
        return "{0}/rest/security/acl/layers/{1}".format(self.geoserver_url,layerrule)
    
    def get_catalogue_mode(self):
        res = self.get(self.catalogue_mode_url(),headers=self.accept_header("json"))
        return res.json()["mode"]
    
    def set_catalogue_mode(self,mode):
        data = CATALOGUE_MODE_TEMPLATE.format(mode)
        res = self.put(self.catalogue_mode_url(),data = data,headers=self.contenttype_header("xml"))
        logger.debug("Succeed to set catalogue mode.")
    
    def get_layer_access_rules(self):
        res = self.get(self.layer_access_rules_url(),headers=self.accept_header("json"))
        return res.json()
    
    def delete_layer_access_rule(self,permission):
        res = self.delete(self.layer_access_rule_url(permission))
        logger.debug("Succeed to delete layer access rules for permission({}).".format(permission))

    def update_layer_access_rules(self,layer_access_rules):
        existing_layer_access_rules = self.get_layer_access_rules()
        new_rules = {}
        update_rules = {}
    
        #delete the not required layer access rules
        for permission,groups in existing_layer_access_rules.items():
            if permission not in layer_access_rules:
                self.delete_layer_access_rule(permission)
            else:
                update_rules[permission] = layer_access_rules[permission]
    
        #get new layer access rules
        for permission,groups in layer_access_rules.items():
            if permission not in update_rules:
                new_rules[permission] = groups
    
        if update_rules:
            data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,v) for k,v in update_rules.items()))
            res = self.put(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
    
        if new_rules:
            data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,v) for k,v in new_rules.items()))
            res = self.post(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
    
        logger.debug("Succeed to update the layer access rules.")
    
