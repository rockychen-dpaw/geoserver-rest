import unittest
import os

from .basetest import BaseTest

class RolesTest(BaseTest):
    def test_roles(self):
        test_group = "group4unitest"
        test_user = "test.user01@unitest.com"
        test_role = "role14unitest"
        try:
            print("Reset the test env")
            self.geoserver.delete_role(test_role)
            self.geoserver.add_usergroup(test_group)
            self.geoserver.update_user(test_user,"1234")
   
            #create role
            print("Test: Create role")
            self.assertTrue(not self.geoserver.has_role(test_role),"The role({}) should not exist".format(test_role))
            self.assertTrue(self.geoserver.add_role(test_role),"The role({}) should not exist".format(test_role))
            self.assertTrue(not self.geoserver.add_role(test_role),"The role({}) should not exist".format(test_role))
            self.assertTrue(self.geoserver.has_role(test_role),"The role({}) should already be created just now".format(test_role))
            self.assertTrue(test_role in self.geoserver.list_roles(),"The role({}) should already be created just now".format(test_role))

            #assign role to usergroup
            print("Test: Associate a role with a user group")
            self.assertTrue(not self.geoserver.get_usergroup_roles(test_group),"The usergroup({}) has no associated roles ".format(test_group))
            self.geoserver.associate_role_with_usergroup(test_role,test_group)
            self.assertEqual(self.geoserver.get_usergroup_roles(test_group),[test_role],"The usergroup({}) has only one associated role '{}' ".format(test_group,test_role))

            #unassign role from usergroup
            print("Test: Unassociate a role from a user group")
            self.geoserver.unassociate_role_with_usergroup(test_role,test_group)
            self.assertTrue(not self.geoserver.get_usergroup_roles(test_group),"The usergroup({}) has no associated roles ".format(test_group))

            #assign role to user
            print("Test: Associate a role with a user")
            self.assertTrue(not self.geoserver.get_user_roles(test_user),"The user({}) has no associated roles ".format(test_user))
            self.geoserver.associate_role_with_user(test_role,test_user)
            self.assertEqual(self.geoserver.get_user_roles(test_user),[test_role],"The user({}) has only one associated role '{}' ".format(test_user,test_role))

            #unassign role from user
            print("Test: Unassociate a role from a user")
            self.geoserver.unassociate_role_with_user(test_role,test_user)
            self.assertTrue(not self.geoserver.get_user_roles(test_user),"The user({}) has no associated roles ".format(test_user))

            #delete role
            print("Test: Delete role")
            self.assertTrue(self.geoserver.delete_role(test_role),"The role({}) should exist before".format(test_role))
            self.assertTrue(not self.geoserver.delete_role(test_role),"The role({}) should already be deleted before".format(test_role))
            self.assertTrue(not self.geoserver.has_role(test_role),"The role({}) should already be deleted".format(test_role))

        except Exception as ex:
            self.geoserver.delete_role(test_role)
            raise ex
        finally:
            self.geoserver.delete_usergroup(test_group)
            self.geoserver.delete_usergroup(test_user)
            pass

if __name__ == "__main__":
    unittest.main()

