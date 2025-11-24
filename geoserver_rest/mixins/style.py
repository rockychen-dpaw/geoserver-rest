import logging

logger = logging.getLogger(__name__)

GENERIC_STYLE_TEMPLATE = """<?xml version="1.0" encoding="ISO-8859-1"?>
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
class StyleMixin(object):
    def styles_url(self,workspace):
        return "{0}/rest/workspaces/{1}/styles".format(self.geoserver_url,workspace)
    
    def style_url(self,workspace,stylename):
        return "{0}/rest/workspaces/{1}/styles/{2}".format(self.geoserver_url,workspace,stylename)

    def list_styles(self,workspace):
        res = self.get(self.styles_url(workspace),headers=self.accept_header("json"))
    
        return [str(s["name"]) for s in (res.json().get("styles") or {}).get("style") or [] ]
    
    def get_style(self,workspace,stylename):
        return self.get(self.style_url(workspace,stylename),headers=self.accept_header("json")).json()["style"]
    
    def get_sld(self,workspace,stylename):
        sldversion = self.get_style(workspace,stylename)["languageVersion"]["version"]
        if sldversion == "1.1.0" or sldversion == "1.1":
            sld_content_type = "application/vnd.ogc.se+xml"
        else:
            sld_content_type = "application/vnd.ogc.sld+xml"
    
        return self.get("{}.sld".format(self.style_url(workspace,stylename)),headers={"Accept": sld_content_type}).text

    def has_style(self,workspace,stylename):
        return self.has(self.style_url(workspace,stylename),headers=self.accept_header("json"))
    
    def delete_style(self,workspace,stylename,recurse=True):
        """
        Return True if deleted;otherwise return False if doesn't exist before
        """
        if not self.has_style(workspace,stylename):
            return False
    
        res = self.delete("{}?recurse={}&purge=true".format(self.style_url(workspace,stylename),"true" if recurse else "false"))
        logger.debug("Succeed to delete the style({}:{})".format(workspace,stylename))
        return True
    
    def update_style(self,workspace,stylename,sldversion,slddata):
        if not self.has_style(workspace,stylename):
            headers = {"content-type": "application/vnd.ogc.sld+xml"}
            placeholder_data = GENERIC_STYLE_TEMPLATE.format(stylename)
            res = self.post(self.styles_url(workspace),data=placeholder_data, headers=headers)
    
        sld_content_type = "application/vnd.ogc.sld+xml"
        if sldversion == "1.1.0" or sldversion == "1.1":
            sld_content_type = "application/vnd.ogc.se+xml"
    
        headers = {"content-type": sld_content_type}
    
        res = self.put(self.style_url(workspace,stylename),data=slddata, headers=headers)
        logger.debug("Succeed to update the style({}:{})".format(workspace,stylename))
    
    def get_style_field(self,styledata,field):
        """
        field:
            name:
            namespace/workspace
            format
            version

        """
        if field in ("namespace","workspace"):
            return styledata.get("workspace",{}).get("name")
        elif field == "version":
            return styledata.get("languageVersion",{}).get("version")
        else:
            return styledata.get(field)
