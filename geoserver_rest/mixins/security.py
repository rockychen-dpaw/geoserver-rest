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
        """
        get/add/update access rules
        """
        return "{0}/rest/security/acl/layers".format(self.geoserver_url)
    
    def layer_access_rule_url(self,layerrule):
        """
        Delete an access rule
        """
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
        """
        permission: a permission or a list of permissions
        Return True if permission is deleted; return False if permission doesn't exist before

        """
        if isinstance(permission,(list,tuple)):
            for p in permission:
                self.delete(self.layer_access_rule_url(p))
        else:
            try:
                res = self.delete(self.layer_access_rule_url(permission))
                return True
            except ResourceNotFound as ex:
                return False

        logger.debug("Succeed to delete layer access rules for permission({}).".format(permission))

    def update_layer_access_rules(self,layer_access_rules):
        """
        update the whole access rules
        layer_access_rules: dict( (permission:roles) )
            permission: "*.*.r"
            roles: a list of group or comma separated roless string. 
               *: means all roles
               NO_ONE : means no roles
        """
        existing_layer_access_rules = self.get_layer_access_rules()
        new_rules = {}
        update_rules = {}
    
        #delete the not required layer access rules
        for permission,roles in existing_layer_access_rules.items():
            if permission not in layer_access_rules:
                self.delete_layer_access_rule(permission)
            else:
                update_rules[permission] = layer_access_rules[permission]
    
        #get new layer access rules
        for permission,roles in layer_access_rules.items():
            if permission not in update_rules:
                new_rules[permission] = roles
    
        if update_rules:
            data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,",".join(v) if isinstance(v,(list,tuple)) else v) for k,v in update_rules.items()))
            res = self.put(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
    
        if new_rules:
            data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,",".join(v) if isinstance(v,(list,tuple)) else v) for k,v in new_rules.items()))
            res = self.post(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
    
        logger.debug("Succeed to update the layer access rules.")
    
    def patch_layer_access_rules(self,layer_access_rules=None,delete_permissions=None):
        """
        patch access rules
        layer_access_rules: dict( (permission:roles) ), add/update the access rules
            permission: "*.*.r"
            roles: a list of group or comma separated roless string. 
               *: means all roles
               NO_ONE : means no roles
        delete_permissions: the permission required to delete
        """
        if layer_access_rules:
            existing_layer_access_rules = self.get_layer_access_rules()
            new_rules = {}
            update_rules = {}
    
            #delete the not required layer access rules
            for permission,roles in existing_layer_access_rules.items():
                if permission in layer_access_rules:
                    update_rules[permission] = layer_access_rules[permission]
        
            #get new layer access rules
            for permission,roles in layer_access_rules.items():
                if permission not in update_rules:
                    new_rules[permission] = roles
        
            if update_rules:
                data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,",".join(v) if isinstance(v,(list,tuple)) else v) for k,v in update_rules.items()))
                res = self.put(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))
        
            if new_rules:
                data = RULES_TEMPLATE.format(os.linesep.join(RULE_TEMPLATE.format(k,",".join(v) if isinstance(v,(list,tuple)) else v) for k,v in new_rules.items()))
                res = self.post(self.layer_access_rules_url(),data=data,headers=self.contenttype_header("xml"))

        if delete_permissions:
            self.delete_layer_access_rule(delete_permissions)

    
        logger.debug("Succeed to patch the layer access rules.")
    
