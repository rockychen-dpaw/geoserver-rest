import unittest
import os

from .basetest import BaseTest

STYLE_TEMPLATE = """<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
                       xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
                       xmlns="http://www.opengis.net/sld" 
                       xmlns:ogc="http://www.opengis.net/ogc" 
                       xmlns:xlink="http://www.w3.org/1999/xlink" 
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>{0}</Name>
    <UserStyle>
      <Name>{0}</Name>
      <Title>A orange generic style</Title>
      <FeatureTypeStyle>
        <Rule>
          <Name>raster</Name>
          <Title>raster</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="isCoverage"/>
              <ogc:Literal>true</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
          </RasterSymbolizer>
        </Rule>
        <Rule>
          <Title>orange polygon</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="dimension">
                <ogc:Function name="geometry"/>
              </ogc:Function>
              <ogc:Literal>2</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <PolygonSymbolizer>
            <Fill>
              <CssParameter name="fill">#ff6600</CssParameter>
            </Fill>
            <Stroke>
              <CssParameter name="stroke">#000000</CssParameter>
              <CssParameter name="stroke-width">0.5</CssParameter>
            </Stroke>
          </PolygonSymbolizer>
        </Rule>
        <Rule>
          <Title>orange line</Title>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:Function name="dimension">
                <ogc:Function name="geometry"/>
              </ogc:Function>
              <ogc:Literal>1</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <LineSymbolizer>
            <Stroke>
              <CssParameter name="stroke">#ff6600</CssParameter>
              <CssParameter name="stroke-opacity">1</CssParameter>
            </Stroke>
          </LineSymbolizer>
        </Rule>
        <Rule>
          <Title>orange point</Title>
          <ElseFilter/>
          <PointSymbolizer>
            <Graphic>
              <Mark>
                <WellKnownName>square</WellKnownName>
                <Fill>
                  <CssParameter name="fill">#ff6600</CssParameter>
                </Fill>
              </Mark>
              <Size>6</Size>
            </Graphic>
          </PointSymbolizer>
        </Rule>
        <VendorOption name="ruleEvaluation">first</VendorOption>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
"""
class FeturetypeTest(BaseTest):
    def test_featuretype(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            test_storename = "testds4unitest"
            parameters = {
                "host": os.environ.get("POSTGIS_HOST"),
                "port": os.environ.get("POSTGIS_PORT"),
                "database": os.environ.get("POSTGIS_DATABASE"),
                "schema": os.environ.get("POSTGIS_SCHEMA"),
                "user": os.environ.get("POSTGIS_USER"),
                "passwd": os.environ.get("POSTGIS_PASSWORD"),
                "Connection timeout": 5,
                "Max connection idle time": 600,
                "min connections": 5,
                "max connections": 20,
                "fetch size": 500
            }
            print("Try to create the datastore({}) for testing".format(test_storename))
            self.geoserver.update_datastore(test_workspace,test_storename,parameters,create=True)
            self.assertTrue(self.geoserver.has_datastore(test_workspace,test_storename),"The datastore({}) should be already created".format(test_storename))
            print("Create the datastore({}) successfully".format(test_storename))

            test_stylenames = ["teststyle14unitest","teststyle24unitest","teststyle34unitest"]
            test_stylenames.sort()
            test_styleversion = "1.0.0"
            for test_stylename in test_stylenames:
                print("Try to create the sld style({0}) for testing".format(test_stylename,test_styleversion))
                test_styledata = STYLE_TEMPLATE.format(test_stylename)
                self.geoserver.update_style(test_workspace,test_stylename,test_styleversion,test_styledata)
                self.assertTrue(self.geoserver.has_style(test_workspace,test_stylename),"The style() should be created".format(test_stylename))
                print("Create the style({}) successfully".format(test_stylename))

            for test_layername,parameters in [(
                "testft4unitest",
                {
                    "title":"test feature type",
                    "description":"for unitest",
                    "keywords":["unitest"],
                    "srs":"EPSG:4326",
                    "nativeName":os.environ["POSTGIS_TABLE"]
                }
            ),(
                "testview4unitest",
                {
                    "title":"test feature type",
                    "description":"for unitest",
                    "keywords":["unitest"],
                    "srs":"EPSG:4326",
                    "viewsql":"select * from {}".format(os.environ["POSTGIS_TABLE"]),
                    "geometry_column":os.environ["POSTGIS_GEOMETRY_COLUMN"],
                    "geometry_type":os.environ["POSTGIS_GEOMETRY_TYPE"]
                }
            )]:
                print("Try to create the testing feature type({})".format(test_layername))
                self.geoserver.publish_featuretype(test_workspace,test_storename,test_layername,parameters,create=True)
                self.assertTrue(self.geoserver.has_featuretype(test_workspace,test_layername,storename=test_storename),"The layer({}:{}) should have been created".format(test_workspace,test_layername))
                self.assertEqual(len(self.geoserver.list_featuretypes(test_workspace,test_storename)),1,"The workspace({}:{}) should only have one layer".format(test_workspace,test_storename))
                self.assertEqual(len(self.geoserver.list_featuretypes(test_workspace)),1,"The workspace({}) should only have one layer".format(test_workspace))
                print("Create the testing feature type({}) successfully".format(test_layername))
                print("Try to update the testing feature type({})".format(test_layername))
                test_featuretype = self.geoserver.get_featuretype(test_workspace,test_layername,storename=test_storename)
                parameters["nativeBoundingBox"] = [test_featuretype["nativeBoundingBox"]["minx"],test_featuretype["nativeBoundingBox"]["miny"],test_featuretype["nativeBoundingBox"]["maxx"],test_featuretype["nativeBoundingBox"]["maxy"],test_featuretype["nativeBoundingBox"].get("crs","EPSG:4326")]
                parameters["latLonBoundingBox"] = [test_featuretype["latLonBoundingBox"]["minx"],test_featuretype["latLonBoundingBox"]["miny"],test_featuretype["latLonBoundingBox"]["maxx"],test_featuretype["latLonBoundingBox"]["maxy"],test_featuretype["latLonBoundingBox"].get("crs","EPSG:4326")]
                self.geoserver.publish_featuretype(test_workspace,test_storename,test_layername,parameters,create=False)
                self.assertTrue(self.geoserver.has_featuretype(test_workspace,test_layername,storename=test_storename),"The layer({}:{}) should exist".format(test_workspace,test_layername))
                print("Update the testing feature type({}) successfully".format(test_layername))
                print("Try to update the testing feature type({}) without parameter 'create'".format(test_layername))
                self.geoserver.publish_featuretype(test_workspace,test_storename,test_layername,parameters)
                self.assertTrue(self.geoserver.has_featuretype(test_workspace,test_layername,storename=test_storename),"The layer({}:{}) should exist".format(test_workspace,test_layername))
                print("Update the testing feature type({}) successfully".format(test_layername))
    
                print("Try to set the styles of the testing feature type({})".format(test_layername))
                self.geoserver.set_layer_styles(test_workspace,test_layername,test_stylenames[0],test_stylenames[1:])
                layer_styles = self.geoserver.get_layer_styles(test_workspace,test_layername)
                self.assertEqual(layer_styles[0][1],test_stylenames[0],"The default style of the testing feature type should be {0} instead of {1}".format(test_stylenames[0],layer_styles[0]))
                layer_styles[1].sort(key=lambda o:o[1])
                self.assertEqual([o[1] for o in layer_styles[1]],test_stylenames[1:],"The alternative styles of the testing feature type should be {0} instead of {1}".format(test_stylenames[1:],layer_styles[1]))
                print("Set the styles of the testing feature type({}) succesfully".format(test_layername))
                
    
                print("Try to delete the testing feature type({})".format(test_layername))
                self.geoserver.delete_featuretype(test_workspace,test_storename,test_layername)
                self.assertFalse(self.geoserver.has_featuretype(test_workspace,test_layername,storename=test_storename),"The layer({}:{}) should have been deleted".format(test_workspace,test_layername))
                self.assertEqual(len(self.geoserver.list_featuretypes(test_workspace,test_storename)),0,"The workspace({}:{}) should be empty".format(test_workspace,test_storename))
                self.assertEqual(len(self.geoserver.list_featuretypes(test_workspace)),0,"The workspace({}) should be empty".format(test_workspace))
                print("Delete the testing feature type({}) succesfully".format(test_layername))

            for test_stylename in test_stylenames:
                print("Try to delete the test style({})".format(test_stylename))
                self.geoserver.delete_style(test_workspace,test_stylename)
                print("Delete the test style({}) successfully".format(test_stylename))
            print("Try to delete the test datastore({})".format(test_storename))
            self.geoserver.delete_datastore(test_workspace,test_storename)
            print("Delete the test datastore({}) successfully".format(test_storename))

            recurse = False

        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=recurse)




if __name__ == "__main__":
    unittest.main()

