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
class StylesMixin(object):
    def styles_url(self,workspace):
        return "{0}/rest/workspaces/{1}/styles".format(self.geoserver_url,workspace)
    
    def style_url(self,workspace,stylename):
        return "{0}/rest/workspaces/{1}/styles/{2}".format(self.geoserver_url,workspace,stylename)
    
    def layer_styles_url(self,workspace,layername):
        return "{0}/rest/layers/{1}:{2}".format(self.geoserver_url,workspace,layername)
    
    def has_style(self,workspace,stylename):
        return self.has(self.style_url(workspace,stylename),headers=self.accept_header("json"))
    
    def delete_style(self,workspace,stylename,recurse=True):
        """
        Return True if deleted;otherwise return False if doesn't exist before
        """
        if not self.has_style(workspace,stylename):
            return False
    
        r = self.delete("{}?recurse={}&purge=true".format(self.style_url(workspace,stylename),"true" if recurse else "false"))
        if r.status_code >= 300:
            raise Exception("Failed to delete the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
    
        logger.debug("Succeed to delete the style({}:{})".format(workspace,stylename))
        return True
    
    def update_style(self,workspace,stylename,sldversion,slddata):
        if not self.has_style(workspace,stylename):
            headers = {"content-type": "application/vnd.ogc.sld+xml"}
            placeholder_data = GENERIC_STYLE_TEMPLATE.format(stylename)
            r = self.post(self.styles_url(workspace),data=placeholder_data, headers=headers)
            if r.status_code >= 300:
                logger.error("Failed to create the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
                raise Exception("Failed to create the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
    
    
        sld_content_type = "application/vnd.ogc.sld+xml"
        if sldversion == "1.1.0" or sldversion == "1.1":
            sld_content_type = "application/vnd.ogc.se+xml"
    
        headers = {"content-type": sld_content_type}
    
        r = self.put(self.style_url(workspace,stylename),data=slddata, headers=headers)
    
        if r.status_code >= 300:
            logger.error("Failed to update the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
            raise Exception("Failed to update the style({}:{}). code = {} , message = {}".format(workspace,stylename,r.status_code, r.content))
    
        logger.debug("Succeed to update the style({}:{})".format(workspace,stylename))
    
    def get_layer_styles(self,workspace,layername):
        """
        Return a tuple(default style, alternate styles)
        """
        r = self.get(self.layer_styles_url(workspace,layername),headers=self.accept_header("json"))
        if r.status_code == 200:
            r = r.json()
            return (r.get("defaultStyle",{}).get("name",None), [d["name"] for d in r.get("styles",{}).get("style",[])])
        else:
            raise Exception("Failed to get styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        LAYER_STYLES_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<layer>
  {0}
  <styles class="linked-hash-set">
      {1}
  </styles>
</layer>
    """
    def set_layer_styles(self,workspace,layername,default_style,styles):
        layer_styles_data = LAYER_STYLES_TEMPLATE.format(
            "<defaultStyle><name>{}</name></defaultStyle>".format(default_style) if default_style else "",
            os.linesep.join("<style><name>{}</name></style>".format(n) for n in styles) if styles else ""
        )
        r = self.put(self.layer_styles_url(workspace,layername),headers=self.contenttype_header("xml"),data=layer_styles_data)
        if r.status_code >= 300:
            raise Exception("Failed to set styles of the featuretype({}:{}). code = {} , message = {}".format(workspace,layername,r.status_code, r.content))
    
        logger.debug("Succeed to set the styles of the layer({}:{}),default_style={}, styles={}".format(workspace,layername,default_style,styles))
    
