import unittest

from .basetest import BaseTest

SLD_1_1_0_TEMPLATE="""<?xml version="1.0" ?>
<StyledLayerDescriptor version="1.1.0" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:se="http://www.opengis.net/se" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd">
    <NamedLayer>
        <se:Name>{0}</se:Name>
        <UserStyle>
            <se:Name>{0}</se:Name>
            <se:FeatureTypeStyle>
                <se:Rule>
                    <se:Name>Single symbol</se:Name>
                    <se:PolygonSymbolizer>
                        <se:Fill>
                            <se:SvgParameter name="fill">#7d6157</se:SvgParameter>
                        </se:Fill>
                        <se:Stroke>
                            <se:SvgParameter name="stroke">#6e6e6e</se:SvgParameter>
                            <se:SvgParameter name="stroke-width">0.1</se:SvgParameter>
                        </se:Stroke>
                    </se:PolygonSymbolizer>
                </se:Rule>
            </se:FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>
"""

SLD1_0_0_TEMPLATE = """<?xml version="1.0" encoding="ISO-8859-1"?>
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
class StyleTest(BaseTest):
    def test_workspace(self):
        test_workspace = "testws4unitest"
        #create the test workspace if doesn't have
        if self.geoserver.has_workspace(test_workspace):
            print("The testing workspace({}) already exist, delete it".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,True)

        print("Create the testing workspace({}) for testing".format(test_workspace))
        self.geoserver.create_workspace(test_workspace)
        recurse = True
        try:
            test_data = (("testStyle1004unitest","1.0.0",SLD1_0_0_TEMPLATE),("testStyle1104unitest","1.1.0",SLD_1_1_0_TEMPLATE))
            for test_stylename,version,template in test_data:
                print("Try to create the sld style({0}) with version({1})".format(test_stylename,version))
                test_styledata = template.format(test_stylename)
                self.geoserver.update_style(test_workspace,test_stylename,version,test_styledata)
                print("Check whether the sld style({0}) with version({1}) was created successfully".format(test_stylename,version))
                test_styledata = template.format(test_stylename)
                self.assertTrue(self.geoserver.has_style(test_workspace,test_stylename),"The style() should be created".format(test_stylename))
                print("The sld style({0}) with version({1}) was created successfully".format(test_stylename,version))
            self.assertEqual(len(self.geoserver.list_styles(test_workspace)),len(test_data),"The workspace({}) should only contain 1 style".format(test_workspace))
            print("Now,the workspace({0}) have {1} new created styles".format(test_workspace,len(test_data)))

            for test_stylename,version,template in test_data:
                print("Try to delete the sld style({0}) with version({1})".format(test_stylename,version))
                self.geoserver.delete_style(test_workspace,test_stylename)
                print("Check whether the sld style({0}) with version({1}) was deleted successfully".format(test_stylename,version))
                self.assertFalse(self.geoserver.has_style(test_workspace,test_stylename),"The style() should not exist".format(test_stylename))
                print("The sld style({0}) with version({1}) was deleted successfully".format(test_stylename,version))

            self.assertEqual(len(self.geoserver.list_styles(test_workspace)),0,"The workspace({}) should have no styles".format(test_workspace))
            print("All styles have been deleted; Now, the workspace({0}) have no styles".format(test_workspace))
            recurse = False
        finally:
            #delete the test workspace
            print("Delete the testing workspace({})".format(test_workspace))
            self.geoserver.delete_workspace(test_workspace,recurse=recurse)




if __name__ == "__main__":
    unittest.main()

