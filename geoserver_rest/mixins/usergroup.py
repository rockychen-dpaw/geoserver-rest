import logging
import collections
import requests

from ..exceptions import *
from .. import settings

logger = logging.getLogger(__name__)

USER_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<user>
	<userName><![CDATA[{0}]]></userName>
	{1}
	{2}
</user>
"""
PASSWORD_TEMPLATE = "<password>{}</password>"
ENABLED_TEMPLATE = "<enabled>{}</enabled>"

class UsergroupMixin(object):
    """
    group don't support the chars % ; / \\ 
    user don't support the chars % ; / \\ <>
    """
    def login_url(self):
        """
        Return True if login successfully; otherwise return False
        """
        return "{0}/rest".format(self.geoserver_url)

    def usergroups_url(self,service=None):
        """
        service: user group service. use the default user group service if service is None
        Return all groups belonging to a user group service.
        """
        if service:
            return "{0}/rest/security/usergroup/service/{1}/groups".format(self.geoserver_url,service)
        else:
            return "{0}/rest/security/usergroup/groups".format(self.geoserver_url)
        
    def usergroup_url(self,group,service=None):
        """
        a url for a specific user group

        """
        if service:
            return "{0}/rest/security/usergroup/service/{1}/group/{2}".format(self.geoserver_url,service,group)
        else:
            return "{0}/rest/security/usergroup/group/{1}".format(self.geoserver_url,self.urlencode(group))
        
    def users_url(self,usergroup=None,service=None):
        """
        default user group includes all users
        if usergroup is None,use the default user group
        if service is None, use the default user group service
        Return the url to get the list of users in usergroup in user group service; if usergroup is None, return url to get the user list in default user group
        """
        if usergroup:
            if service:
                return "{0}/rest/security/usergroup/service/{1}/group/{2}/users".format(self.geoserver_url,service,self.urlencode(usergroup))
            else:
                return "{0}/rest/security/usergroup/group/{1}/users".format(self.geoserver_url,self.urlencode(usergroup))
        else:
            if service:
                return "{0}/rest/security/usergroup/service/{1}/users".format(self.geoserver_url,service)
            else:
                return "{0}/rest/security/usergroup/users".format(self.geoserver_url)

    def user_url(self,user,service=None):
        """
        modify/delete a user
        if service is None, use the default user group service
        """
        if service:
            return "{0}/rest/security/usergroup/service/{1}/user/{2}".format(self.geoserver_url,service,self.urlencode(user))
        else:
            return "{0}/rest/security/usergroup/user/{1}".format(self.geoserver_url,self.urlencode(user))

    def user_groups_url(self,user,service=None):
        """
        Query all groups for a user

        """
        if service:
            return "{0}/rest/security/usergroup/service/{1}/user/{1}/groups".format(self.geoserver_url,service,self.urlencode(user))
        else:
            return "{0}/rest/security/usergroup/user/{1}/groups".format(self.geoserver_url,self.urlencode(user))

    def user_group_url(self,user,group,service=None):
        """
        Associate/unassociate a user with/from a group
        """
        if service:
            return "{0}/rest/security/usergroup/service/{1}/user/{2}/group/{3}".format(self.geoserver_url,service,self.urlencode(user),group)
        else:
            return "{0}/rest/security/usergroup/user/{1}/group/{2}".format(self.geoserver_url,self.urlencode(user),group)

    def list_usergroups(self,service=None):
        """
        Return list of groups in usergroup
        """
        res = self.get(self.usergroups_url(service=service),headers=self.accept_header("json"))
        return res.json().get("groups") or []

    def has_usergroup(self,group,service=None):
        return any(g for g in self.list_usergroups(service=service) if g == group)

    def add_usergroup(self,group,service=None):
        """
        Return True if added,return False if already exist
        """
        try:
            res = self.post(self.usergroup_url(group,service=service),None,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("json")))
            logger.debug("Succeed to add the usergroup({}).".format(group))
            return True
        except requests.RequestException as ex:
            if self.has_usergroup(group):
                return False
            else:
                raise ex

    def delete_usergroup(self,group,service=None):
        """
        Return True if delete,return False if doesn't exist before
        """
        #r = self.delete(self.usergroup_url(group),headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("json")))
        try:
            res = self.delete(self.usergroup_url(group,service=service)) 
            return True
        except ResourceNotFound as ex:
            return False

    def list_users(self,usergroup=None,service=None):
        """
        Return list of users(username,enabled) in usergroup; if usergroup is None, return the user list in default user group
        """
        res = self.get(self.users_url(usergroup,service=service),headers=self.accept_header("json"))
        print("***users={}".format(res.json()))
    
        return [(u["userName"],u["enabled"]) for u in (res.json().get("users") or [])]

    def has_user(self,user,service=None):
        return any(u for u in self.list_users(service=service) if u[0] == user)

    def get_user(self,user,service=None):
        try:
            return next(u for u in self.list_users(service=service) if u[0] == user)
        except StopIteration as ex:
            return ObjectNotFound("The user({}) does not exist.".format(user))

    def create_user(self,user,password,enable=True,service=None):
        return self.update_user(user,password=password,enable=enable,create=True,service=service)

    def enable_user(self,user,enable,service=None):
        return self.update_user(user,password=None,enable=enable,create=False,service=service)

    def change_userpassword(self,user,password,service=None):
        return self.update_user(user,password=password,enable=None,create=False,service=service)

    def update_user(self,user,password=None,enable=None,create=None,service=None):
        """
        create/update user
        Return True if added;otherwise return False if already exist
        """
        if create is None:
            create = False if self.has_user(user,service=service) else True

        if create:
            if enable is None:
                enable = True
            if password is None:
                password = ""

        user_data = USER_TEMPLATE.format(user,PASSWORD_TEMPLATE.format(password) if password is not None else "" , ENABLED_TEMPLATE.format("true" if enable else "false") if enable is not None else "")

        print("****{} = {}".format(user,user_data))

        if create:
            res = self.post(self.users_url(service=service),user_data,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")))
            logger.debug("Succeed to add the user({}).".format(user))
            return True
        else:
            res = self.post("{}.json".format(self.user_url(user,service=service)),user_data,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")))
            logger.debug("Succeed to update the user({}).".format(user))
            return False

    def delete_user(self,user,service=None):
        """
        Return True if user was deleted; otherwise return False if user doesn't exist before
        """
        try:
            res = self.delete("{}.json".format(self.user_url(user,service=service)),headers=self.accept_header("json"))
            logger.debug("Succeed to delete the user({}).".format(user))
            return True
        except ResourceNotFound as ex:
            return False

    def login(self,user,password):
        if self.headers:
            headers = collections.ChainMap(self.accept_header("json"),self.headers)
        else:
            headers = self.accept_header("json")
        res = requests.get(self.login_url() , headers=headers, auth=(user,password),timeout=settings.REQUEST_TIMEOUT,allow_redirects=False)
        if res.status_code == 401:
            #authenticate failed
            return False
        elif res.status_code < 400 or res.status_code >=500:
            #failed to authenticate the user
            res.raise_for_status()
        else:
            #authenticated, but failed to process the request
            return True

    def list_user_groups(self,user,service=None):
        try:
            res = self.get(self.user_groups_url(user,service=service),headers=self.accept_header("json"))
            return res.json().get("groups") or []
        except ResourceNotFound as ex:
            return []

    def user_in_group(self,user,group,service=None):
        """
        Return True if user is in the group; otherwise return False
        """
        return any(True for d in self.list_user_groups(user,service=service) if d == group)

    def add_user_to_group(self,user,group,service=None):
        """
        Return True if user was added to group; otherwise return False if user alreay existed in that group
        """
        res = self.post(self.user_group_url(user,group,service=service),None)
        logger.debug("Succeed to add the user({}) to the group.".format(user,group))
        return True

    def delete_user_from_group(self,user,group,service=None):
        """
        Return True if user was deleted from group; otherwise return False if user didn't exist in that group
        """
        res = self.delete(self.user_group_url(user,group,service=service))

        logger.debug("Succeed to remove the user({}) from the group.".format(user,group))
        return True
        
    def update_user_groups(self,user,groups=None,service=None):
        """
        Update the groups which the user is belonging to

        """
        existing_groups = self.list_user_groups(user,service=service)
        if groups:
            for g in existing_groups:
                if g in groups:
                    continue
                self.delete_user_from_group(user,g,service=service)
            for g in groups:
                if g in existing_groups:
                    continue
                self.add_user_to_group(user,g,service=service)
        else:
            for g in existing_groups:
                self.delete_user_from_group(user,g,service=service)
        
