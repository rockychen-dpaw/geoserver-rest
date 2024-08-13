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

class UsergroupMixin(object):
    """
    group don't support the chars % ; / \ 
    user don't support the chars % ; / \ <>
    """
    def usergroups_url(self):
        return "{0}/rest/security/usergroup/groups".format(self.geoserver_url)
        
    def usergroup_url(self,group):
        return "{0}/rest/security/usergroup/group/{1}".format(self.geoserver_url,encode(group))
        
    def users_url(self,usergroup=None):
        """
        Return the url to get the list of users in usergroup; if usergroup is None, return url to get the user list in default user group
        """
        if usergroup:
            return "{0}/rest/security/usergroup/group/{1}/users".format(self.geoserver_url,encode(usergroup))
        else:
            return "{0}/rest/security/usergroup/users".format(self.geoserver_url)

    def user_url(self,user):
        return "{0}/rest/security/usergroup/user/{1}".format(self.geoserver_url,encode(user))

    def user_groups_url(self,user):
        return "{0}/rest/security/usergroup/user/{1}/groups".format(self.geoserver_url,encode(user))

    def user_group_url(self,user,group):
        return "{0}/rest/security/usergroup/user/{1}/group/{2}".format(self.geoserver_url,encode(user),encode(group))

    def list_usergroups(self):
        """
        Return list of groups in usergroup
        """
        res = self.get(self.usergroups_url(),headers=self.accept_header("json"))
        return res.json().get("groups") or []

    def has_usergroup(self,group):
        return any(g for g in self.list_usergroups() if g == group)

    def add_usergroup(self,group):
        """
        Return True if added,return False if already exist
        """
        try:
            res = self.post("{}.json".format(self.usergroup_url(group)),None,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("json")))
            logger.debug("Succeed to add the usergroup({}).".format(group))
            return True
        except requests.RequestException as ex:
            if self.has_usergroup(group):
                return False
            else:
                raise ex

    def delete_usergroup(self,group):
        """
        Return True if delete,return False if doesn't exist before
        """
        #r = self.delete("{}.json".format(self.usergroup_url(group)),headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("json")))
        try:
            res = self.delete("{}.json".format(self.usergroup_url(group))) 
            return True
        except ResourceNotFound as ex:
            return False

    def list_users(self,usergroup=None):
        """
        Return list of users(username,enabled) in usergroup; if usergroup is None, return the user list in default user group
        """
        res = self.get(self.users_url(usergroup),headers=self.accept_header("json"))
    
        return [(u["userName"],u["enabled"]) for u in (res.json().get("users") or [])]

    def has_user(self,user,usergroup=None):
        return any(u for u in self.list_users() if u[0] == user)

    def update_user(self,user,password,enabled=True,create=None):
        """
        create/update user
        Return True if added;otherwise return False if already exist
        """
        user_data = USER_TEMPLATE.format(user,password or "" , "true" if enabled else "false")
        if create is None:
            create = False if self.has_user(user) else True
        if create:
            res = self.post(self.users_url(),user_data,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")))
            logger.debug("Succeed to add the user({}).".format(user))
            return True
        else:
            res = self.post("{}.json".format(self.user_url(user)),user_data,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("xml")))
            logger.debug("Succeed to update the user({}).".format(user))
            return False

    def delete_user(self,user):
        """
        Return True if user was deleted; otherwise return False if user doesn't exist before
        """
        try:
            res = self.delete("{}.json".format(self.user_url(user)),headers=self.accept_header("json"))
            logger.debug("Succeed to delete the user({}).".format(user))
            return True
        except ResourceNotFound as ex:
            return False

    def list_user_groups(self,user):
        try:
            res = self.get(self.user_groups_url(user),headers=self.accept_header("json"))
            return res.json().get("groups") or []
        except ResourceNotFound as ex:
            return []

    def add_user_to_group(self,user,group):
        """
        Return True if user was added to group; otherwise return False if user alreay existed in that group
        """
        res = self.post("{}.json".format(self.user_group_url(user,group)),None,headers=collections.ChainMap(self.accept_header("json"),self.contenttype_header("json")))
        logger.debug("Succeed to add the user({}) to the group.".format(user,group))
        return True

    def delete_user_from_group(self,user,group):
        """
        Return True if user was deleted from group; otherwise return False if user didn't exist in that group
        """
        res = self.delete("{}.json".format(self.user_group_url(user,group)),headers=self.accept_header("json"))

        logger.debug("Succeed to remove the user({}) from the group.".format(user,group))
        return True
        
    def update_user_groups(self,user,groups=None):
        existing_groups = self.list_user_groups(user)
        if groups:
            for g in existing_groups:
                if g in groups:
                    continue
                self.delete_user_from_group(user,g)
            for g in groups:
                if g in existing_groups:
                    continue
                self.add_user_to_group(user,g)
        else:
            for g in existing_groups:
                self.delete_user_from_group(user,g)
        
