import logging
import os

logger = logging.getLogger(__name__)

GEOPACKAGE_CONNECTION_PARAMETERS = {
    "Primary key metadata table": "",
    "Callback factory":	"",
    "Evictor tests per run": 3,
    "database": "",
    "Batch insert size": 1,
    "fetch size": 1000,
    "Connection timeout": 20,
    "namespace": "",
    "max connections": 10,
    "Test while idle": True,
    "Max connection idle time":	300,
    "Session startup SQL": "",
    "validate connections": True,
    "dbtype": "geopkg",
    "passwd": "",
    "Expose primary keys": True,
    "min connections": 1,
    "Evictor run periodicity": 300,
    "Session close-up SQL":"",
    "user": ""
}
    
POSTGIS_CONNECTION_PARAMETERS = {
    "Connection timeout": 20,
    "validate connections": True,
    "port": 5432,
    "Primary key metadata table":"",
    "Support on the fly geometry simplification": True,
    "create database": False,
    "create database params":"",
    "dbtype": "postgis",
    "Batch insert size": 1,
    "namespace":"",
    "Max connection idle time": 300,
    "Session startup SQL":"",
    "Expose primary keys": True,
    "min connections": 1,
    "Max open prepared statements": 50,
    "Callback factory":"",
    "passwd": "",
    "encode functions": False,
    "host": "localhost",
    "Evictor tests per run": 3,
    "Loose bbox": True,
    "Evictor run periodicity": 300,
    "Estimated extends": True,
    "database": "",
    "fetch size": 1000,
    "Test while idle": True,
    "max connections": 10,
    "preparedStatements": False,
    "Session close-up SQL":"",
    "schema": "public",
    "user": "",
    "SSL mode":"ALLOW"
}

SHAPEFILE_CONNECTION_PARAMETERS = {
    "cache and reuse memory maps": True,
    "namespace": "",
    "filetype": "shapefile",
    "charset": "ISO-8859-1",
    "create spatial index": True,
    "fstype": "shape",
    "url":"",
    "enable spatial index": True,
    "memory mapped buffer": False,
    "timezone": "Australian Western Standard Time"
}

STORE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<dataStore>
    <name>{}</name>
    <description>{}</description>
    <connectionParameters>
{}
    </connectionParameters>
</dataStore>
"""
CONNECTION_PARAMETER_TEMPLATE = """        <entry key="{}">{}</entry>"""
class DatastoreMixin(object):
    def datastores_url(self,workspace):
        return "{0}/rest/workspaces/{1}/datastores".format(self.geoserver_url,workspace)
    
    def datastore_url(self,workspace,storename):
        return "{0}/rest/workspaces/{1}/datastores/{2}".format(self.geoserver_url,workspace,storename)

    def upload_dataset_url(self,workspace,storename,filename,method="file",dataformat=None,update="overwrite",configure="none"):
        """
        method : The upload method. Can be "url", "file", "external". “file” uploads a file from a local source. The body of the request is the file itself. “url” uploads a file from an remote source. The body of the request is a URL pointing to the file to upload. This URL must be visible from the server. “external” uses an existing file on the server. The body of the request is the absolute path to the existing file.
        dataformat: The type of source data store (e.g., “shp”).
        filename: The target file name for the file to be uploaded
        configure: The configure parameter can take one of the following values:
          first: (Default) Sets up only the first feature type available in the data store.
          none: Does not configure any feature types.
          all: Configures all feature types. 
        """
        if not dataformat:
            if datasetname.rsplit(".",1)[1].lower() in ("gpkg","geopackage"):
                dataformat = "gpkg"
            elif datasetname.rsplit(".",1)[1].lower() in ("zip","shp"):
                dataformat = "shp"
            else:
                raise Exception("Can't determine the data format'")
        return "{0}/rest/workspaces/{1}/datastores/{2}/{4}.{5}?filename={3}&update={6}&configure={7}".format(self.geoserver_url,workspace,storename,filename,method,dataformat,update,configure)

    
    def has_datastore(self,workspace,storename):
        return self.has(self.datastore_url(workspace,storename))
    
    def get_datastore(self,workspace,storename):
        return self.get(self.datastore_url(workspace,storename)).json().get("dataStore",{})

    def list_datastores(self,workspace):
        """
        Return the list of datastores
        """
        res = self.get(self.datastores_url(workspace), headers=self.accept_header("json"))
    
        return [str(d["name"]) for d in (res.json().get("dataStores") or {}).get("dataStore") or [] ]

    def upload_dataset(self,workspace,storename,file,filename=None,dataformat=None,update="overwrite"):
        """
        Upload the dataset and create the datastore it doesn't exist
        """
        if not dataformat:
            if file.rsplit(".",1)[1].lower() in ("gpkg","geopackage"):
                dataformat = "gpkg"
            elif file.rsplit(".",1)[1].lower() in ("zip","shp"):
                dataformat = "shp"
            else:
                raise Exception("Can't determine the data format'")
        if not filename:
            filename = os.path.split(file)[1]

        with open(file,'rb') as f:
            res = self.put(
                self.upload_dataset_url(workspace,storename,filename,method="file",dataformat=dataformat,update=update),
                f,
                headers=self.contenttype_header(dataformat),timeout=600)

    def update_datastore(self,workspace,storename,parameters,create=None):
        """
        Return True if created; otherwise return False if update
        """
        ds_connection_parameters = None
        if any(k for k in ["host","port","schema"] if k in parameters):
            ds_connection_parameters = POSTGIS_CONNECTION_PARAMETERS
        elif parameters.get("database","").endswith(".gpkg"):
            ds_connection_parameters = GEOPACKAGE_CONNECTION_PARAMETERS
        else:
            url = parameters.get("url")
            if not url:
                raise Exception("Can't recognize the datastore type")
            if url.endswith(".shp"):
                ds_connection_parameters = SHAPEFILE_CONNECTION_PARAMETERS
            else:
                raise Exception("The datastore({}) Not Support".format(url))
    
        connection_parameters = {}
        for k,v in ds_connection_parameters.items():
            if k in parameters:
                value = parameters[k]
            else:
                value = v

            if value is None:
                continue
            elif isinstance(value,bool):
                value = "true" if value else "false"
            else:
                value = str(value)
            connection_parameters[k] = value
    
        store_data = STORE_TEMPLATE.format(
            storename,parameters.get("description",""),
            os.linesep.join(CONNECTION_PARAMETER_TEMPLATE.format(k,v) for k,v in connection_parameters.items())
        )
        if create is None:
            #check whether datastore exists or not.
            create = False if self.has_datastore(workspace,storename) else True
    
        if create:
            res = self.post(self.datastores_url(workspace),data=store_data,headers=self.contenttype_header("xml"))
            logger.debug("Succeed to create the datastore({}:{})".format(workspace,storename))
            return True
        else:
            res = self.put(self.datastore_url(workspace,storename),data=store_data,headers=self.contenttype_header("xml"))
            logger.debug("Succeed to update the datastore({}:{})".format(workspace,storename))
            return False

    def delete_datastore(self,workspace,storename,recurse=False):
        """
        Return true if deleted the datastore; otherwise return False if datastore doesn't exist
        """
        if not self.has_datastore(workspace,storename):
            logger.debug("The datastore({}:{}) doesn't exists".format(workspace,storename))
            return False
    
        res = self.delete("{}?recurse={}".format(self.datastore_url(workspace,storename),"true" if recurse else "false"))
        logger.debug("Succeed to delete the datastore({}:{})".format(workspace,storename))
        return True
    
    def get_datastore_field(self,storedata,field):
        """
        field:
            name:
            enabled:
            description:
            workspace
            datastore related parameters

        Get the wms field from wms json data, returned by get_wmsstore
        """
        if field == "workspace":
            return storedata.get("workspace",{}).get("name")
        elif field == "description":
            return storedata.get("description")
        else:
            return next((item['$'] for item in storedata.get("connectionParameters",{}).get("entry",[]) if item['@key'] == field)  ,None)
