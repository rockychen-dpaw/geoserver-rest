import unittest
import os

from .basetest import BaseTest

class UsergroupTest(BaseTest):
    def atest_usergroup(self):
        test_groups = ["_group4unitest"]
        try:
            for g in test_groups:
                self.geoserver.delete_usergroup(g)

            existing_groups = self.geoserver.list_usergroups()
            print("Geoserver has {} existing usergroups.".format(len(existing_groups)))

            for g in test_groups:
                print("Try to add the usergroup({})".format(g))
                self.geoserver.add_usergroup(g)
                self.assertTrue(self.geoserver.has_usergroup(g),"The usergroup({}) should been created".format(g))
                print("Add the usergroup({}) sucessfully".format(g))
            usergroups = self.geoserver.list_usergroups()
            self.assertEqual(len(usergroups),len(test_groups) + len(existing_groups),"The geoserver should have {} usergroups instead of {} usergroups.".format(len(test_groups) + len(existing_groups),len(usergroups)))

            for g in test_groups:
                print("Try to delete the usergroup({})".format(g))
                self.assertTrue(self.geoserver.delete_usergroup(g),"The usergroup({}) should exist before".format(g))
                self.assertFalse(self.geoserver.has_usergroup(g),"The usergroup({}) should be deleted".format(g))
                self.assertFalse(self.geoserver.delete_usergroup(g),"The usergroup({}) should already been deleted before".format(g))

            self.assertEqual(len(self.geoserver.list_usergroups()), len(existing_groups),"All the test groups should been deleted")
        except Exception as ex:
            for g in test_groups:
                try:
                    self.geoserver.delete_usergroup(g)
                except:
                    pass
            raise ex
        

    def test_user(self):
        test_groups = ["_group14unitest","_group24unitest","_group34unitest"]
        test_groups.sort()
        test_users = ["_user4unitest","_user4unitest@.!,~'\"+-^?`|#$&=","_user4unitest()","_user4unitest[]","_user4unitest<>","_user4unitest{}","test.user01@unitest.com"]
        #test_users = ["_user4unitest"]

        try:
            for g in test_groups:
                print("Try to create the usergroup({}) for testing".format(g))
                self.geoserver.add_usergroup(g)
                print("Create the usergroup({}) successfully".format(g))

            for u in test_users:
                try:
                    self.geoserver.delete_user(u)
                except:
                    pass

            existing_users = self.geoserver.list_users()
            print("Geoserver has {} existing users({}).".format(len(existing_users),existing_users))

            for u in test_users:
                print("Try to add the user({})".format(u))
                self.assertTrue(self.geoserver.create_user(u,"1234",enable=True),"The user({}) should be a new user".format(u))
                self.assertTrue(self.geoserver.has_user(u),"The user({}) should been created".format(u))
                self.assertTrue(self.geoserver.login(u,"1234"),"Failed to login the user({}) ".format(u))
                print("Add the user({}) sucessfully. users in default group = {}".format(u,self.geoserver.list_users()))

                print("Try to disable the user({})".format(u))
                self.assertFalse(self.geoserver.enable_user(u,False),"The user({}) should be a existing user".format(u))
                print("Disable the user({}) sucessfully".format(u))

                print("Try to enable the user({})".format(u))
                self.assertFalse(self.geoserver.enable_user(u,True),"The user({}) should be a existing user".format(u))
                self.assertTrue(self.geoserver.has_user(u),"The user({}) should been created".format(u))
                print("Enable the user({}) sucessfully".format(u))

                print("Try to change the password of the user({})".format(u))
                self.assertFalse(self.geoserver.change_userpassword(u,"123456"),"The user({}) should already exist".format(u))
                self.assertTrue(self.geoserver.login(u,"123456"),"Failed to login the user({}) ".format(u))
                print("Change the password of the user({}) sucessfully".format(u))


                print("Try to add the user({}) to groups({})".format(u,test_groups[:2]))
                self.geoserver.update_user_groups(u,test_groups[:2])
                groups = self.geoserver.get_user_groups(u)
                groups.sort()
                self.assertEqual(groups,test_groups[:2],"The user({}) should belong to the groups({}) instead of groups({})".format(u,test_groups[:2],groups))
                self.assertTrue(self.geoserver.user_in_group(u,test_groups[0]),"The user({}) should be in the group({})".format(u,test_groups[0]))
                print("Add the user({0}) to group({1}) sucessfully. users in default group = {2}, users in group({1}) = {3}".format(u,test_groups[0],self.geoserver.list_users(),self.geoserver.list_users(test_groups[0])))

                print("Try to add the user({}) to groups({})".format(u,test_groups[1:]))
                self.geoserver.update_user_groups(u,test_groups[1:])
                groups = self.geoserver.get_user_groups(u)
                groups.sort()
                self.assertEqual(groups,test_groups[1:],"The user({}) should belong to the groups({}) instead of groups({})".format(u,test_groups[1:],groups))
                print("Remove the user({0}) from group({1}) sucessfully. users in default group = {2}, users in group({1}) = {3}".format(u,test_groups[0],self.geoserver.list_users(),self.geoserver.list_users(test_groups[0])))

                print("Try to remove the user({}) from all groups".format(u))
                self.geoserver.update_user_groups(u)
                groups = self.geoserver.get_user_groups(u)
                self.assertEqual(len(groups),0,"The user({}) should not belong to any group".format(u))

            users = self.geoserver.list_users()
            self.assertEqual(len(users),len(test_users) + len(existing_users),"The geoserver should have {} users instead of {} users.".format(len(test_users) + len(existing_users),len(users)))
            return
            for u in test_users:
                print("Try to delete the user({})".format(u))
                self.assertTrue(self.geoserver.delete_user(u),"The user({}) should exist before".format(u))
                self.assertFalse(self.geoserver.has_user(u),"The user({}) should be deleted".format(u))
                self.assertFalse(self.geoserver.delete_user(u),"The user({}) should already been deleted before".format(u))

            self.assertEqual(len(self.geoserver.list_users()), len(existing_users),"All the test users should been deleted")

        except Exception as ex:
            for u in test_users:
                try:
                    self.geoserver.delete_user(u)
                    pass
                except:
                    pass
            raise ex
        finally:
            for g in test_groups:
                try:
                    #self.geoserver.delete_usergroup(g)
                    pass
                except:
                    pass

if __name__ == "__main__":
    unittest.main()

