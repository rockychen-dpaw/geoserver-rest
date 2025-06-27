import logging
import collections
import urllib.parse
import requests

from ..exceptions import *

logger = logging.getLogger(__name__)

USER_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<user>
	<userName><![CDATA[{0}]]></userName>
	<password>{1}</password>
	<enabled>{2}</enabled>
</user>
"""
def encode(s):
    s = urllib.parse.quote(s)
    return s

class RolesMixin(object):
    def roles_url(self,service=None):
        """
        if service is None, use default role service
        Query all roles
        """
        if service:
            return "{0}/rest/security/roles/service/{1}/roles/".format(self.geoserver_url,service)
        else:
            return "{0}/rest/security/roles".format(self.geoserver_url)

    def role_url(self,role,service):
        """
        if service is None, use default role service
        The url to add/delete a role
        """
        if service:
            return "{0}/rest/security/service/{1}/role/{2}".format(self.geoserver_url,service,role)
        else:
            return "{0}/rest/security/roles/role/{1}".format(self.geoserver_url,role)

    def user_roles_url(self,user,service=None):
        """
        if service is None, use default role service
        Query all roles for a user
        """
        if service:
            return "{0}/rest/security/roles/service/{1}/user/{2}".format(self.geoserver_url,service,self.urlencode(user))
        else:
            return "{0}/rest/security/roles/user/{1}".format(self.geoserver_url,self.urlencode(user))

    def user_role_url(self,role,user,service=None):
        """
        if service is None, use default role service
        The url to associate/unassociate a role with/from a user
        """
        if service:
            return "{0}/rest/security/service/{1}/roles/role/{2}/user/{3}".format(self.geoserver_url,service,role,self.urlencode(user))
        else:
            return "{0}/rest/security/roles/role/{1}/user/{2}".format(self.geoserver_url,role,self.urlencode(user))

    def usergroup_roles_url(self,group,service=None):
        """
        if service is None, use default role service
        Query all roles for a usergroup
        """
        if service:
            return "{0}/rest/security/roles/service/{1}/group/{2}".format(self.geoserver_url,service,group)
        else:
            return "{0}/rest/security/roles/group/{1}".format(self.geoserver_url,group)

    def usergroup_role_url(self,role,group,service=None):
        """
        if service is None, use default role service
        The url to associate/unassociate a role with/from a usergroup
        """
        if service:
            return "{0}/rest/security/service/{1}/roles/role/{2}/group/{3}".format(self.geoserver_url,service,role,group)
        else:
            return "{0}/rest/security/roles/role/{1}/group/{2}".format(self.geoserver_url,role,group)

    def list_roles(self,service=None):
        """
        if service is None, use default role service
        Return all roles
        """
        res = self.get(self.roles_url(service=service),headers=self.accept_header("json"))
        return res.json().get("roles") or []

    def has_role(self,role,service=None):
        """
        if service is None, use default role service
        Return True if role exists; otherwise return False
        """
        return any(r for r in self.list_roles(service=service) if r == role)

    def add_role(self,role,service=None):
        """
        Add a role
        if service is None, use default role service
        Return True if added; False if role already exists
        """
        try:
            res = self.post(self.role_url(role,service=service),None)
            return True
        except Exception as ex:
            if self.has_role(role):
                return False
            else:
                raise ex

    def delete_role(self,role,service=None):
        """
        Delete a role
        if service is None, use default role service
        Return True if deleted; False if role didn't exist before'
        """
        try:
            self.delete(self.role_url(role,service=service))
            return True
        except ResourceNotFound as ex:
            return False

    def get_user_roles(self,user):
        """
        Return the list of roles for user
        """
        res = self.get("{}.json".format(self.user_roles_url(user)),headers=self.accept_header("json"))
        return res.json().get("roles",[])

    def user_has_rule(self,user,role):
        """
        Return True if user has the role; otherwise return False
        """
        return any(True for r in self.get_user_roles(user) if r == role)

    def get_usergroup_roles(self,group):
        """
        Return the list of roles for user
        """
        res = self.get(self.usergroup_roles_url(group),headers=self.accept_header("json"))
        return res.json().get("roles",[])

    def usergroup_has_rule(self,usergroup,role):
        """
        Return True if the usergroup has the role; otherwise return False
        """
        return any(True for r in self.get_usergroup_roles(usergroup) if r == role)


    def associate_role_with_user(self,role,user,service=None):
        """
        if service is None, use default role service
        Associate a role to a user
        """
        self.post("{}.json".format(self.user_role_url(role,user,service=service)),None,headers=self.accept_header("json"))

    def unassociate_role_with_user(self,role,user,service=None):
        """
        if service is None, use default role service
        Associate a role to a user
        """
        self.delete("{}.json".format(self.user_role_url(role,user,service=service)),headers=self.accept_header("json"))

    def associate_role_with_usergroup(self,role,group,service=None):
        """
        if service is None, use default role service
        Associate a role to a user group
        """
        self.post(self.usergroup_role_url(role,group,service=service),None)

    def unassociate_role_with_usergroup(self,role,group,service=None):
        """
        if service is None, use default role service
        Associate a role to a user group
        """
        self.delete(self.usergroup_role_url(role,group,service=service))

