import os
import json
import logging
import math
import jinja2
import subprocess
from datetime import timedelta

from .geoserver import Geoserver
from . import timezone
from . import settings
from . import loggingconfig
from . import utils

logger = logging.getLogger("geoserver_rest.gwccachemanage")

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader([settings.BASE_DIR]),
    autoescape=jinja2.select_autoescape()
)


class GWCManage(object):
    def __init__(self,geoserver_name,geoserver_url,geoserver_user,geoserver_password,ssl_verify,gwc_tiles_dir,gwc_disk_size,requestheaders=None):
        self.geoserver_name = geoserver_name
        self.gwc_tiles_dir = gwc_tiles_dir
        self.gwc_disk_size = gwc_disk_size
        if self.gwc_disk_size:
            if self.gwc_disk_size[-1] in ("G","g"):
                self.gwc_disk_size = int((self.gwc_disk_size)[:-1]) * 1024 * 1024
            elif self.gwc_disk_size[-1] in ("M","m"):
                self.gwc_disk_size = int((self.gwc_disk_size)[:-1]) * 1024
            elif self.gwc_disk_size[-1] in ("K","k"):
                self.gwc_disk_size = int((self.gwc_disk_size)[:-1])
            else:
                raise Exception("The disk capacity unit '" + self.gwc_disk_size[-1] + "' Not Support.")
        else:
            self.gwc_disk_size = 0

        self.gwccleanstatusfile = os.path.join(self.gwc_tiles_dir,"gwccleanstatus.json")
        self.gwclayersfile = os.path.join(settings.REPORT_HOME,"gwclayers.html")
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=requestheaders,ssl_verify=ssl_verify)
        self._layers = None


    def get_gwc_diskusage_status(self):
        """
        unit is K
        Return disksize,used size ; otherwise return None if failed
        """
        result = subprocess.run("df --output='size,used' -BK \"{}\"".format(self.gwc_tiles_dir), capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            #it is a mounted volume
            data = result.stdout.splitlines()[1].split()
            logger.debug("Found the gwc tiles disk usage mounted on folder '{}' via command 'df': {}".format(self.gwc_tiles_dir,data))
            return (int(data[0][:-1]),int(data[1][:-1]))

        result = subprocess.run("du  -k -d 0 \"{}\"".format(self.gwc_tiles_dir), capture_output=True, text=True, shell=True)
        if result.returncode != 0:
            #gwc tile dir is not a mounted volume
            logger.debug("Failed to find the disk usage of the folder({}) via du.{}".format(self.gwc_tiles_dir, result.stderr))
            return None
        data = int(result.stdout.split()[0])
        logger.debug("Found the gwc tiles disk usage on folder '{}' via command 'du': {}".format(self.gwc_tiles_dir,data))
        return (self.gwc_disk_size,data)

    def load_cleaning_status(self):
        """
        Load cleaning status of gwc layers
        """
        #load current layer disk usage 
        if self._layers is not None:
            #already loaded,return directly
            return 

        gwccleanstatus = None
        if (os.path.exists(self.gwccleanstatusfile)):
            try:
                with open(self.gwccleanstatusfile,"r") as f:
                    gwccleanstatus = json.loads(f.read())
            except Exception as ex:
                logger.error("Failed to load layer clean status file '{}'.{}".format(self.gwccleanstatusfile ,str(ex)))
                gwccleanstatus = {}
        else:
            gwccleanstatus = {}

        #find all gwc layers
        self._layers = []
        layer = None
        for workspace,name in self.geoserver.list_gwclayers():
            metadata = self.geoserver.get_gwclayer(workspace,name)
            layer = {
                "name": [workspace,name],
                "expireCache": int(self.geoserver.get_gwclayer_field(metadata,"expireCache") or 0)
            }

            layer.update(gwccleanstatus.get(workspace,{}).get(name,{}))
            self._layers.append(layer)

        return None

    def clean_layer_cache(self,layer,emergency=False):
        """
        Return True if some expired tiles are removed; return False if no tiles are removed

        """
        cache_starttime = None
        #if cleaned before, get the cache starttime
        if "cache_starttime" in layer:
            try:
                cache_starttime = timezone.parse(layer["cache_starttime"],"%Y-%m-%d %H:%M:%S.%f")
            except:
                pass

        try:
            now = timezone.localtime()
            starttime = now
            layer["clean_starttime"] = timezone.format(starttime,pattern="%Y-%m-%d %H:%M:%S.%f")
            layer["clean_emergency"] = emergency
            layer["clean_message"] = "Succeed"
            layer["clean_succeed"] = True
            minutes = 0
            layer_tile_dir = "{}/{}_{}".format(self.gwc_tiles_dir,layer["name"][0],layer["name"][1])
            expireCache = layer["expireCache"]
            if os.path.exists(layer_tile_dir):
                if expireCache <= 0 :
                    #no expire time
                    layer["clean_message"] = "'expireCache' is 0, skip cleaning.'".format(layer["name"][0],layer["name"][1])
                    return False
    
                if emergency and cache_starttime:
                    #clean tiles which were created in the earliest time 10% of expireCache
                    minutes = math.floor(((now - cache_starttime).total_seconds() - int(expireCache * 0.1) ) / 60)
                else:
                    if expireCache > 86400:
                        #2 hours buffer
                        minutes = int(expireCache / 60) + 120
                    elif expireCache > 3600:
                        #1 hours buffer
                        minutes = int(expireCache / 60) + 60
                    else:
                        #10 mins buffer
                        minutes = int(expireCache / 60) + 10
    
                    if cache_starttime and (now - cache_starttime).total_seconds() <= minutes * 60:
                        #cache_starttime is later than the planned clean time, no need to clean
                        layer["clean_message"] = "The cache start time is later than the clean time, skip."
                        return False
                if minutes <= 0:
                    #will clean all tiles.
                    layer["clean_message"] = "Attempt to delete all tiles, skip."
                    return False

                cmd = "find {} -type f -mmin +{} -delete".format(layer_tile_dir,minutes)
                logger.debug("Try to delete the old tiles with command: {}".format(cmd))
                result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
                layer["cache_starttime"] = timezone.format(now - timedelta(seconds = minutes * 60),pattern="%Y-%m-%d %H:%M:%S.%f")
                if result.returncode != 0:
                    raise Exception(result.stderr)
                #successfully delete some older tiles
                return True
            elif expireCache > 0 :
                layer["cache_starttime"] = timezone.format(now,pattern="%Y-%m-%d %H:%M:%S.%f")
                return False
            else:
                if "cache_starttime" in layer:
                    del layer["cache_starttime"]

                return False

        except Exception as ex:
            layer["clean_message"] = "{}: {}".format(ex.__class__.__name__,str(ex))
            layer["clean_succeed"] = False
            return False
        finally:
            #find the cache size of the layer after cleaning(succeed or not)
            if os.path.exists(layer_tile_dir):
                cmd = "du -k -d 0 \"{}\"".format(layer_tile_dir)
                result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
                if result.returncode != 0:
                    layer["clean_message"] = "Failed to get the cache size for layer({}:{}).{}".format(layer["name"][0],layer["name"][1],result.stderr)
                else:
                    layer["cache_size"] = int(result.stdout.split()[0])
            else:
                layer["cache_size"] = 0

            endtime = timezone.localtime()
            layer["clean_endtime"] = timezone.format(endtime,pattern="%Y-%m-%d %H:%M:%S.%f")
            layer["clean_processtime"]= (endtime - starttime).total_seconds()
            if settings.DEBUG:
                if layer.get("clean_succeed"):
                    logger.debug("Succeed to clean the gwc cache of the layer '{}:{}'. {}".format(layer["name"][0],layer["name"][1],layer.get("clean_message")))
                else:
                    logger.debug("Failed to clean the gwc cache of the layer '{}:{}'. {}".format(layer["name"][0],layer["name"][1],layer.get("clean_message")))

    def get_diskusage(self,diskusagestatus):
        """
        Get the disk usage data of gwc cache
        """
        diskusage = {}
        #get disk usage data
        if diskusagestatus:
            if self.gwc_disk_size > 0 and self.gwc_disk_size < diskusagestatus[0]:
                diskusage["disksize"] = self.gwc_disk_size
            else:
                diskusage["disksize"] = diskusagestatus[0]
            diskusage["cachesize"] = diskusagestatus[1]
        else:
            if self.gwc_disk_size > 0:
                diskusage["disksize"] = self.gwc_disk_size
            else:
                diskusage["disksize"] = 0
            diskusage["cachesize"] = -1

        #format disk usage data
        if diskusage["disksize"] <= 0:
            diskusage["percent"] = "N/A"
            diskusage["disksize"] = "Unlimited"
            if diskusage["cachesize"]  == -1:
                diskusage["cachesize"] = "N/A"
            elif diskusage["cachesize"] / 1048576 >= 10:
                #more than 10G
                diskusage["cachesize"] = "{:,.2f}G".format(diskusage["cachesize"] / 1048576)
            elif diskusage["cachesize"] / 1024 >= 10:
                #more than 10M
                diskusage["cachesize"] = "{:,.2f}M".format(diskusage["cachesize"] / 1024)
            else:
                diskusage["cachesize"] = "{:,}K".format(diskusage["cachesize"])
        else:
            diskusage["percent"] = "{:.2f}%".format(diskusage["cachesize"] * 100 / diskusage["disksize"])

            if diskusage["disksize"] / 1048576 >= 10:
                #more than 10G
                diskusage["disksize"] = "{:,.2f}G".format(diskusage["disksize"] / 1048676)
            elif diskusage["disksize"] / 1024 >= 10:
                diskusage["disksize"] = "{:,.2f}M".format(diskusage["disksize"] / 1024)
            else:
                diskusage["disksize"] = "{:,}K".format(diskusage["disksize"])

            if diskusage["cachesize"] / 1048576 >= 10:
                #more than 10G
                diskusage["cachesize"] = "{:,.2f}G".format(diskusage["cachesize"] / 1048576)
            elif diskusage["cachesize"] / 1024 >= 10:
                diskusage["cachesize"] = "{:,.2f}M".format(diskusage["cachesize"] / 1024)
            else:
                diskusage["cachesize"] = "{:,}K".format(diskusage["cachesize"])


        diskusage["checktime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S.%f")

        return diskusage

    def post_clean(self):
        """
        Save the cleaning status
        Update gwclayers.html
        """
        gwccleanstatus = {}
        cachesize = 0
        for layer in self._layers:
            if "clean_starttime" not in layer:
                continue
            if layer["name"][0] not in gwccleanstatus:
                gwccleanstatus[layer["name"][0]] = {}
            gwccleanstatus[layer["name"][0]][layer["name"][1]] = dict([(k,v) for k,v in layer.items() if k.startswith("clean") or k.startswith("cache_")])
            if "cache_size" not in layer:
                layer["cache_size_human"] = "?"
            elif layer.get("cache_size") / 1048576 > 1:
                #more than 1G
                layer["cache_size_human"] = "{:,.2f}G".format(layer["cache_size"] / 1048576)
            elif layer.get("cache_size") / 1024 > 1:
                #more than 1M
                layer["cache_size_human"] = "{:,.2f}M".format(layer["cache_size"] / 1024)
            else:
                layer["cache_size_human"] = "{:,}K".format(layer["cache_size"])

            layer["expireCache_human"] = utils.format_timedelta(layer.get("expireCache",0),'s')

            cachesize += layer.get("cache_size",0)

        #save gwccleanstatus
        try:
            with open(self.gwccleanstatusfile,"w") as f:
                if settings.DEBUG:
                    f.write(json.dumps(gwccleanstatus,indent=4))
                else:
                    f.write(json.dumps(gwccleanstatus))
        except Exception as ex:
            logger.error("Failed to save layer clean status file '{}'.{}".format(self.gwccleanstatusfile,str(ex)))

        diskusage = self.get_diskusage(self.get_gwc_diskusage_status())

        #sort the layers on cache_size
        self._layers.sort(key=lambda o:o.get("cache_size",0),reverse=True)
        #generate the reports.html
        reports_template = jinja_env.get_template("gwclayers.html")
        with open(self.gwclayersfile,"w") as f:
            f.write(reports_template.render({
                "geoserver" : self.geoserver_name,
                "diskusage" : diskusage,
                "layers":self._layers
            }))
        logger.debug("Succeed to populate the gwclayers.html({})".format(self.gwclayersfile))

    def clean_gwc_cache(self,gwc_cleantime=0):
        self.load_cleaning_status()

        #sort the layers
        self._layers.sort(key=lambda o:o["name"])

        #find the latest cleanbatchid if have
        lastcleanbatchid = None
        for layer in self._layers:
            if layer.get("cleanbatchid"):
                if not lastcleanbatchid:
                    lastcleanbatchid = layer.get("cleanbatchid")
                elif lastcleanbatchid < layer.get("cleanbatchid"):
                    #Found a new clean batch
                    lastcleanbatchid = layer.get("cleanbatchid")
                else:
                    #the clean batch of this layer is a previous clean batch
                    break

        started = timezone.localtime()
        if lastcleanbatchid:
            #already have a started cleanbatchid, finish it first.
            leading_layers = True
            for layer in self._layers:
                if layer.get("cleanbatchid") == lastcleanbatchid:
                    leading_layers = False
                    if layer.get("clean_succeed",False):
                        #already cleaned before
                        continue
                elif leading_layers:
                    #layer is before the layers which cleanbatchid is lastcleanbatchid
                    continue

                layer["cleanbatchid"] = lastcleanbatchid
                self.clean_layer_cache(layer)
                if gwc_cleantime > 0 and (timezone.localtime() - started).total_seconds() > gwc_cleantime:
                    #done
                    break

        if gwc_cleantime <= 0 or (timezone.localtime() - started).total_seconds() < gwc_cleantime:
            #still have time,continue clean other layers
            cleanbatchid = timezone.format(started,pattern="%Y-%m-%d %H:%M:%S.%f")
            for layer in self._layers:
                if  layer.get("clean_starttime") and layer.get("clean_starttime") >= cleanbatchid:
                    #already cleaned with previous cleanbatchid, no need to clean it again
                    layer["cleanbatchid"] = cleanbatchid
                    continue
                layer["cleanbatchid"] = cleanbatchid
                self.clean_layer_cache(layer)
                if gwc_cleantime > 0 and (timezone.localtime() - started).total_seconds() > gwc_cleantime:
                    #done
                    break

        self.post_clean()

    def check_gwc_cache(self,threadhold=0.9):
        """
        Check the diskuage usage status of gwc cache
        start a emergency cleaning if run out of space,
        Return the number of emergency cleaning rounds
        """
        #get disk usage data
        index = 0
        cleantimes = 0
        cleanrounds = 0
        diskusagestatus = None

        nomore_cleaning = True
        while True:
            diskusagestatus = self.get_gwc_diskusage_status()
            if not diskusagestatus:
                logger.warning("GWC cache checking function is disable.")
                break

            if self.gwc_disk_size and self.gwc_disk_size < diskusagestatus[0]:
                if diskusagestatus[1] / self.gwc_disk_size < threadhold:
                    #lower than the threadhold
                    break
            else:
                if diskusagestatus[1] / diskusagestatus[0] < threadhold:
                    #lower than the threadhold
                    break

            #greater than the threadhold, trigger a emergency clean
            if cleantimes == 0:
                #first time. load the gwc layers
                self.load_cleaning_status()
                self._layers.sort(key=lambda o:o.get("cache_size",0),reverse=True)

            if self.clean_layer_cache(self._layers[index],True):
                #some older tiles were cleaned,
                #can continue to clean more older tiles if necessary
                nomore_cleaning = False
            index += 1
            cleantimes += 1
            if index == len(self._layers):
                #already reach the last element,but the disk usage still really high
                #start the another round emergency cleaning until disk usage is lower than the threadhold
                cleanrounds += 1
                if nomore_cleaning:
                    #already deleted all possible tiles,
                    logger.debug("No layer's gwc cache was cleaned in the last clean round. stop")
                    break
                elif cleanrounds < 9:
                    index = 0
                    nomore_cleaning = True
                    logger.debug("Already cleaned {}0%(time based) of tiles from all gwc layers. begin to start the next round of cleaning.".format(cleanrounds))
                else:
                    logger.debug("Already cleaned 90%(time based) of tiles from all gwc layers. stop.")
                    break

        if cleantimes > 0:
            self.post_clean()
        elif diskusagestatus:
            #update the diskusagestatus
            diskusage = self.get_diskusage(diskusagestatus)
            diskusagestr = """<span id=\\"totaldiskusage\\" style=\\"font-size:14px;font-style:italic;color:#682424;white-space: pre-wrap;\\">Disk Size: {disksize}    Used Disk Size: {cachesize}    Used Percentage: {percent}    Check Time: {checktime}<\\/span>""".format(**diskusage)

            cmd = """sed -i -e "s/<span id=\\"totaldiskusage\\"[^<]*<\\/span>/{}/" {}""".format(
                diskusagestr,
                self.gwclayersfile
            )
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if result.returncode != 0:
                #gwc tile dir is not a mounted volume
                logger.error("Failed to update gwc layer file '{}'.{}".format(self.gwclayersfile,result.stderr))

        return cleantimes

