import unittest

from .basetest import BaseTest

class WorkspaceTest(BaseTest):
    def test_workspace(self):
        if self.geoserver.has_workspace("testws4unitest"):
            print("The testing workspace 'testws4unitest' already exists.delete it for testing")
            self.geoserver.delete_workspace("testws4unitest")

        original_workspaces = self.geoserver.list_workspaces()
        print("The original workspaces are {}".format(original_workspaces))

        print("Try to create the testing workspace 'testws4unitest'")
        self.assertTrue(self.geoserver.create_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should be newly created.")
        self.assertFalse(self.geoserver.create_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should already exist.")
        self.assertTrue(self.geoserver.has_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should exist")
        workspaces = self.geoserver.list_workspaces()
        self.assertEqual(len(workspaces),len(original_workspaces) + 1,"The new workspace list({1}) should have one more workspace than the original workspace({0})".format(original_workspaces,workspaces))
        print("Try to delete the testing workspace 'testws4unitest'")
        self.assertTrue(self.geoserver.delete_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should exist before deleting.")
        self.assertFalse(self.geoserver.delete_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should be already deleted")
        self.assertFalse(self.geoserver.has_workspace("testws4unitest"),"The testing workspace 'testws4unitest' should not exist")

        workspaces = self.geoserver.list_workspaces()
        self.assertEqual(len(workspaces),len(original_workspaces) ,"The new workspace list({1}) should be equal with the original workspace({0})".format(original_workspaces,workspaces))




if __name__ == "__main__":
    unittest.main()

