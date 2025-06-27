import unittest

from .basetest import BaseTest

CATALOGUE_MODES=["HIDE","MIXED","CHALLENGE"]
class SecurityTest(BaseTest):
    def test_catalogue_mode(self):
        mode = self.geoserver.get_catalogue_mode()
        self.assertTrue(mode in CATALOGUE_MODES,"The mode({}) is invalid".format(mode))
        print("The original catalogue mode of the geoserver({0}) is {1}".format(self.geoserver.geoserver_url,mode))
        new_mode = next(m for m in CATALOGUE_MODES if m != mode)
        print("Try to set the catalogue mode of the geoserver({0}) to {1}".format(self.geoserver.geoserver_url,new_mode))
        self.geoserver.set_catalogue_mode(new_mode)
        #retrieve the mode
        setted_mode = self.geoserver.get_catalogue_mode()
        self.assertEqual(setted_mode,new_mode,"The catalogue mode({1}) of the geoserver({0}) is not equal with the expected mode({2}) ".format(self.geoserver.geoserver_url,setted_mode,new_mode))

        #reset the mode to original mode
        print("Try to set the catalogue mode of the geoserver({0}) to original mode {1}".format(self.geoserver.geoserver_url,mode))
        self.geoserver.set_catalogue_mode(mode)
        #retrieve the mode
        setted_mode = self.geoserver.get_catalogue_mode()
        self.assertEqual(setted_mode,mode,"The catalogue mode({1}) of the geoserver({0}) is not equal with the original mode({2}) ".format(self.geoserver.geoserver_url,setted_mode,mode))


        self.geoserver.set_catalogue_mode(mode)
        print("The catalogue mode of the geoserver({0}) is {1}".format(self.geoserver.geoserver_url,mode))

    def test_layer_access_rules(self):
        original_rules = self.geoserver.get_layer_access_rules()
        print("The original layer access rules are {}".format(original_rules))
        test_workspaces = ["testws14unitest","testws24unitest","testws34unitest"]
        for w in test_workspaces:
            self.geoserver.create_workspace(w)
        test_keys = []

        for key,permission in original_rules.items():
            if any(key.startswith(w) for w in test_workspaces):
                test_keys.append(key)
        if test_keys:
            print("The layer access rules() for test workspace already exsits, delete them first.".format((k,v) for k,v in original_rules.items() if key in test_keys))
            for k in test_keys:
                del original_rules[k]
            self.geoserver.update_layer_access_rules(original_rules)

        latest_rules = dict(original_rules)
        try:
            print("Add the testing rules for testing workspaces")
            new_rules = dict()
            for w in test_workspaces[:-1]:
                new_rules["{}.*.r".format(w)] = "*"
                new_rules["{}.*.w".format(w)] = "NO_ONE"
            self.geoserver.patch_layer_access_rules(new_rules)
            latest_rules.update(new_rules)
            rules = self.geoserver.get_layer_access_rules()
            self.assertEqual(rules,latest_rules,"The layer access rules({1}) of the geoserver({0}) is not equal with the expected rules({2}) ".format(self.geoserver.geoserver_url,rules,new_rules))
            print("Succeed to patch the access rules:patched rules = {}".format(new_rules))

            print("Add/update/delete the testing rules for testing workspaces")
            #delete the layer access rules for first testing workspace
            delete_permissions = []
            delete_permissions.append("{}.*.r".format(test_workspaces[0]))
            del new_rules["{}.*.r".format(test_workspaces[0])]
            del latest_rules["{}.*.r".format(test_workspaces[0])]

            delete_permissions.append("{}.*.w".format(test_workspaces[0]))
            del new_rules["{}.*.w".format(test_workspaces[0])]
            del latest_rules["{}.*.w".format(test_workspaces[0])]

            #update the layer access rules for second testing workspace
            new_rules["{}.*.r".format(test_workspaces[1])] = "NO_ONE"
            new_rules["{}.*.w".format(test_workspaces[1])] = "*"
            #add the layer access rules for the last testing workspace
            new_rules["{}.*.r".format(test_workspaces[-1])] = "*"
            new_rules["{}.*.w".format(test_workspaces[-1])] = "NO_ONE"

            latest_rules.update(new_rules)
            self.geoserver.patch_layer_access_rules(new_rules,delete_permissions=delete_permissions)
            rules = self.geoserver.get_layer_access_rules()
            self.assertEqual(rules,latest_rules,"The layer access rules({1}) of the geoserver({0}) is not equal with the expected rules({2}) ".format(self.geoserver.geoserver_url,rules,new_rules))
            print("Succeed to patch the access rules:patched rules = {} , delete_permissions = {}".format(new_rules,delete_permissions))

        finally:
            print("Set the layer access rules to original rules {}".format(original_rules))
            self.geoserver.update_layer_access_rules(original_rules)
            rules = self.geoserver.get_layer_access_rules()
            self.assertEqual(rules,original_rules,"The layer access rules({1}) of the geoserver({0}) is not equal with the original rules({2}) ".format(self.geoserver.geoserver_url,rules,original_rules))
            print("Delete the testing workspaces")
            for w in test_workspaces:
                self.geoserver.delete_workspace(w,True)


if __name__ == "__main__":
    unittest.main()

