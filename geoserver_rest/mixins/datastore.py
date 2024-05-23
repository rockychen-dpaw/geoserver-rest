import logging
import os

logger = logging.getLogger(__name__)

class DatastoreMixin(object):
    def datastores_url(self,workspace):
        return "{0}/rest/workspaces/{1}/datastores".format(self.geoserver_url,workspace)
    
    def datastore_url(self,workspace,storename):
        return "{0}/rest/workspaces/{1}/datastores/{2}".format(self.geoserver_url,workspace,storename)
    
    def has_datastore(self,workspace,storename):
        return self.has(self.datastore_url(workspace,storename))
    
    def list_datastores(self,workspace):
        """
        Return the list of datastores
        """
        r = self.get(self.datastores_url(workspace), headers=self.accept_header("json"))
        if r.status_code >= 300:
            raise Exception("Failed to list the datastores in workspace({}). code = {},message = {}".format(workspace,r.status_code, r.content))
    
        return [str(d["name"]) for d in (r.json().get("dataStores") or {}).get("dataStore") or [] ]

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
    "charset": "ISO-8859-1"
    "create spatial index": True,
    "fstype": "shape"
    "url":"",
    "enable spatial index": True,
    "memory mapped buffer": False,
    "timezone": "Australian Western Standard Time"
}

    SHAPEFILES_CONNECTIONS_PARAMETERS = {
    "cache and reuse memory maps": True,
    "namespace": "",
    "filetype": "shapefile",
    "charset": "ISO-8859-1",
    "create spatial index": True,
    "fstype": "shape"
    "url": "",
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
    """.format(storename,parameters.get("description",""),connection_parameters)
    def update_datastore(self,workspace,storename,parameters,create=None):
        """
        Return True if created; otherwise return False if update
        """
        ds_connection_parameters = None
        if any(k for k in ["host","port","schema"] if k in parameters):
            ds_connection_parameters = POSTGIS_CONNECTION_PARAMETERS
        elif parameters.get("database","").endswith(".gpkg"):
            ds_connection_parameters = POSTGIS_CONNECTION_PARAMETERS
        else:
            url = parameters.get("url")
            if not url:
                raise Exception("Can't recognize the datastore type")
            if url.endswith(".shp"):
                ds_connection_parameters = SHAPEFILE_CONNECTION_PARAMETERS
            else:
                ds_connection_parameters = SHAPEFILE_CONNECTION_PARAMETERS

    
        connection_parameters = None
        for k,v in ds_connection_parameters.items():
            if k in parameters:
                if parameters[k] is None:
                    continue
                else:
                    value = parameters[k]
            else:
                value = v

            if value is None:
                continue
            elif isinstance(value,bool)
                value = "true" if value else "false"
            else:
                value = str(value)
    
            if connection_parameters:
                connection_parameters = """{}{}<entry key="{}">{}</entry>""".format(connection_parameters,os.linesep,k,value)
            else:
                connection_parameters = """<entry key="{}">{}</entry>""".format(k,value)
    
        store_data = STORE_TEMPLATE.format(storename,parameters.get("description",""),connection_parameters)
        if create is None:
            #check whether datastore exists or not.
            create = False if self.has_datastore(workspace,storename) else True
    
        if create:
            r = self.post(self.datastores_url(workspace),data=store_data,headers=self.contenttype_header("xml"))
        else:
            r = self.put(self.datastore_url(workspace,storename),data=store_data,headers=self.contenttype_header("xml"))

        if r.status_code >= 300:
            raise Exception("Failed to {} the datastore({}:{}). code = {},message = {}".format("create" if create else "update",workspace,storename,r.status_code, r.content))
    
        if create:
            logger.debug("Succeed to create the datastore({}:{})".format(workspace,storename))
            return True
        else:
            logger.debug("Succeed to update the datastore({}:{})".format(workspace,storename))
            return False
    
    def delete_datastore(workspace,storename,recurse=False):
        """
        Return true if deleted the datastore; otherwise return False if datastore doesn't exist
        """
        if not self.has_datastore(workspace,storename):
            logger.debug("The datastore({}:{}) doesn't exists".format(workspace,storename))
            return False
    
        r = self.delete("{}?recurse={}".format(self.datastore_url(workspace,storename),"true" if recurse else "false"))
        if r.status_code >= 300:
            raise Exception("Failed to delete datastore({}:{}). code = {} , message = {}".format(workspace,storename,r.status_code, r.content))
    
        logger.debug("Succeed to delete the datastore({}:{})".format(workspace,storename))
        return True
    
