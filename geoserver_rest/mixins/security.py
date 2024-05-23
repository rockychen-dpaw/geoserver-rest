import logging

logger = logging.getLogger(__name__)

class SecurityMixin(object):
    def catalogue_mode_url(self):
        return "{0}/rest/security/acl/catalog".format(self.geoserver_url)
    
    def layer_access_rules_url(self):
        return "{0}/rest/security/acl/layers".format(self.geoserver_url)
    
    def layer_access_rule_url(self,layerrule):
        return "{0}/rest/security/acl/layers/{1}".format(self.geoserver_url,layerrule)
    
    def get_catalogue_mode(self):
        r = self.get(self.catalogue_mode_url(),headers=self.accept_header("json"))
        if r.status_code == 200:
            r = r.json()
            return r["mode"]
        else:
            raise Exception("Failed to get catalogue mode. code = {} , message = {}".format(r.status_code, r.content))
    
    CATALOGUE_MODE_TEMPLATE="""<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <mode>{}</mode>
</catalog>"""
    def set_catalogue_mode(self,mode):
        data=CATALOGUE_MODE_TEMPLATE.format(mode)
        r = self.put(self.catalogue_mode_url(),data = data,headers=self.contenttype_header("xml"))
        if r.status_code >= 300:
            raise Exception("Failed to set the catalogue mode({}). code = {} , message = {}".format(mode,r.status_code, r.content))
    
        logger.debug("Succeed to set catalogue mode.")
    
    def get_layer_access_rules(self):
        r = self.get(self.layer_access_rules_url(),headers=self.accept_header("json"))
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception("Failed to get catalogue mode. code = {} , message = {}".format(r.status_code, r.content))
    
    def delete_layer_access_rule(self,permission):
        r = self.delete(self.layer_access_rule_url(permission))
        if r.status_code >= 300:
            raise Exception("Failed to delete layer access rules for permission({}). code = {} , message = {}".format(permission,r.status_code, r.content))
    
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
            data="""<?xml version="1.0" encoding="UTF-8"?>
<rules>
    {}
</rules>""".format(os.linesep.join("""<rule resource="{0}">{1}</rule>""".format(k,v) for k,v in update_rules.items()))
            r = self.put(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
            if r.status_code >= 300:
                raise Exception("Failed to update layer access rules({}). code = {} , message = {}".format(update_rules,r.status_code, r.content))
    
        if new_rules:
            data="""<?xml version="1.0" encoding="UTF-8"?>
<rules>
    {}
</rules>""".format(os.linesep.join("""<rule resource="{0}">{1}</rule>""".format(k,v) for k,v in new_rules.items()))
            r = self.post(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
            if r.status_code >= 300:
                raise Exception("Failed to create layer access rules({}). code = {} , message = {}".format(new_rules,r.status_code, r.content))
    
        logger.debug("Succeed to update the layer access rules.")
    
