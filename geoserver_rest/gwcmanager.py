import os
import traceback
import json
import logging
import math
import jinja2
import subprocess
import psutil
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

class RunOutofTimeException(Exception):
    def __init__(self):
        super().__init__("Reach the maximum cleaning time, exit now.")

class GWCManager(object):
    KEY_MANAGEMENTSTATUS = "_managementstatus_"
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

        self.gwcmanagementstatusfile = os.path.join(self.gwc_tiles_dir,"gwcmanagementstatus.json")
        self.gwclayersfile = os.path.join(settings.REPORT_HOME,"gwclayers.html")
        self.geoserver = Geoserver(geoserver_url,geoserver_user,geoserver_password,headers=requestheaders,ssl_verify=ssl_verify)
        self._layers = None
        self._managementstatus = None


    def get_diskinfo(self):
        """
        Using psutil or du to get gwc disk size
        unit is K
        Return disksize,used size ; otherwise return None if failed
        """
        
        ispartition = False
        for partition in psutil.disk_partitions(all=True):
            if partition.mountpoint == self.gwc_tiles_dir:
                ispartition = True

        if ispartition:
            diskusage  = psutil.disk_usage(self.gwc_tiles_dir)
            size = int(diskusage.total / 1024)
            used = int(diskusage.used / 1024)
            if self.gwc_disk_size > 0 and self.gwc_disk_size < size:
                size = self.gwc_disk_size
            return (size,used)
        else:
            #it is a normal folder, using du to calculate the used size
            #can't get the maximum size, using 0 
            result = subprocess.run("du  -k -d 0 \"{}\"".format(self.gwc_tiles_dir), capture_output=True, text=True, shell=True)
            if result.returncode != 0:
                #gwc tile dir is not a mounted volume
                logger.debug("Failed to find the disk usage of the folder({}) via du.{}".format(self.gwc_tiles_dir, result.stderr))
                return None
            data = int(result.stdout.split()[0])
            logger.debug("Found the gwc tiles disk usage on folder '{}' via command 'du': {}".format(self.gwc_tiles_dir,data))
            return (self.gwc_disk_size if self.gwc_disk_size > 0 else 0,data)

    def load_cleaning_status(self):
        """
        Load cleaning status
        1. overall clean status
        2. layer's clean status
        """
        #load current layer disk usage 
        if self._layers is not None:
            #already loaded,return directly
            return 

        gwcmanagementstatus = None
        if (os.path.exists(self.gwcmanagementstatusfile)):
            try:
                with open(self.gwcmanagementstatusfile,"r") as f:
                    gwcmanagementstatus = json.loads(f.read())
            except Exception as ex:
                logger.error("Failed to load layer clean status file '{}'.{}".format(self.gwcmanagementstatusfile ,str(ex)))
                gwcmanagementstatus = {}
        else:
            gwcmanagementstatus = {}

        self._managementstatus = gwcmanagementstatus.get(self.KEY_MANAGEMENTSTATUS,{})

        #find all gwc layers
        self._layers = []
        layer = None
        for workspace,name in self.geoserver.list_gwclayers():
            metadata = self.geoserver.get_gwclayer(workspace,name)
            layer = {
                "name": [workspace,name],
                "expireCache": int(self.geoserver.get_gwclayer_field(metadata,"expireCache") or 0)
            }
            layer[self.KEY_MANAGEMENTSTATUS] = gwcmanagementstatus.get(workspace,{}).get(name,{})
            self._layers.append(layer)


    def clean_layer_cache(self,layer,emergency=False,cleanround=0):
        """
        Clean layer's gwc cache
        cleanround: only used in emergencyclean. based on 1
        Return True if more tiles can be deleted; return False if no more tiles can be deleted 

        """
        managementstatus = layer[self.KEY_MANAGEMENTSTATUS]
        now = timezone.localtime()
        starttime = now
        try:
            managementstatus["clean_starttime"] = timezone.format(starttime,pattern="%Y-%m-%d %H:%M:%S")
            managementstatus["clean_emergency"] = emergency
            managementstatus["clean_message"] = "Succeed"
            managementstatus["clean_succeed"] = True

            cache_starttime = managementstatus.get("cache_starttime")
            if cache_starttime:
                cache_starttime = timezone.parse(cache_starttime,"%Y-%m-%d %H:%M:%S")

            minutes = 0
            layer_tile_dir = "{}/{}_{}".format(self.gwc_tiles_dir,layer["name"][0],layer["name"][1])
            expireCache = layer["expireCache"]
            if os.path.exists(layer_tile_dir):
                #has cached tiles
                if expireCache <= 0 :
                    #expireCache is disabled
                    layer["clean_message"] = "'expireCache' is 0, skip.'".format(layer["name"][0],layer["name"][1])
                    return False

                if emergency:
                    #clean 10% oldest tiles based on tile expire time
                    seconds = expireCache * (10 - cleanround) / 10
                    minutes = math.floor(seconds / 60)
                    delete2time = now - timedelta(minutes=minutes)
                    if cache_starttime and cache_starttime >= delete2time:
                        #no tiles to delete in this round, but still have tiles to delete in the next round.
                        layer["clean_message"] = "No tiles are required to delete on the round of emergency clean, skip."
                        return True
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
                        #cache_starttime is later than the planned clean time, no need to clean in the normal clean
                        layer["clean_message"] = "The cache start time is later than the clean time, skip."
                        return False

                cmd = "find {} -type f -mmin +{} -delete".format(layer_tile_dir,minutes)
                logger.debug("Try to delete the expired or oldest tiles with command: {}".format(cmd))
                result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
                managementstatus["cache_starttime"] = timezone.format(now - timedelta(minutes = minutes),pattern="%Y-%m-%d %H:%M:%S")
                if result.returncode != 0:
                    raise Exception(result.stderr)
                #successfully delete some older tiles
                return True
            elif expireCache > 0 :
                #don't have cached tiles, but expireCache is enabled
                layer["clean_message"] = "No tiles are cached , skip."
                managementstatus["cache_starttime"] = timezone.format(now,pattern="%Y-%m-%d %H:%M:%S")
                return False
            else:
                #don't have cached tiles, but expireCache is disabled
                layer["clean_message"] = "'expireCache' is 0 and no tiles are cached, skip.'".format(layer["name"][0],layer["name"][1])
                if "cache_starttime" in managementstatus:
                    del managementstatus["cache_starttime"]

                return False

        except Exception as ex:
            managementstatus["clean_message"] = traceback.format_exc()
            managementstatus["clean_succeed"] = False
            return False
        finally:
            endtime = timezone.localtime()
            managementstatus["clean_endtime"] = timezone.format(endtime,pattern="%Y-%m-%d %H:%M:%S")
            managementstatus["clean_processtime"]= int((endtime - starttime).total_seconds())
            if settings.DEBUG:
                if managementstatus.get("clean_succeed"):
                    logger.debug("Succeed to clean the gwc cache of the layer '{}:{}'. {}".format(layer["name"][0],layer["name"][1],managementstatus.get("clean_message")))
                else:
                    logger.debug("Failed to clean the gwc cache of the layer '{}:{}'. {}".format(layer["name"][0],layer["name"][1],managementstatus.get("clean_message")))

    def get_diskusagedata(self,diskinfo):
        """
        Get the disk usage data of gwc cache
        """
        diskusage = {}
        #get disk usage data
        if diskinfo:
            diskusage["disksize"] = diskinfo[0]
            diskusage["usedsize"] = diskinfo[1]
        else:
            if self.gwc_disk_size > 0:
                diskusage["disksize"] = self.gwc_disk_size
            else:
                diskusage["disksize"] = 0
            diskusage["usedsize"] = -1

        #format disk usage data
        if diskusage["disksize"] <= 0:
            diskusage["percent"] = "N/A"
            diskusage["disksize"] = "Unknown"
            if diskusage["usedsize"]  == -1:
                diskusage["usedsize"] = "N/A"
            elif diskusage["usedsize"] / 1048576 >= 1:
                #more than 1G
                diskusage["usedsize"] = "{:,.2f}G".format(diskusage["usedsize"] / 1048576)
            elif diskusage["usedsize"] / 1024 >= 1:
                #more than 1M
                diskusage["usedsize"] = "{:,.2f}M".format(diskusage["usedsize"] / 1024)
            else:
                diskusage["usedsize"] = "{:,}K".format(diskusage["usedsize"])
        else:
            diskusage["percent"] = "{:.2f}%".format(diskusage["usedsize"] * 100 / diskusage["disksize"])

            if diskusage["disksize"] / 1048576 >= 1:
                #more than 1G
                diskusage["disksize"] = "{:,.2f}G".format(diskusage["disksize"] / 1048676)
            elif diskusage["disksize"] / 1024 >= 1:
                #more than 1M
                diskusage["disksize"] = "{:,.2f}M".format(diskusage["disksize"] / 1024)
            else:
                diskusage["disksize"] = "{:,}K".format(diskusage["disksize"])

            if diskusage["usedsize"] / 1048576 >= 1:
                #more than 1G
                diskusage["usedsize"] = "{:,.2f}G".format(diskusage["usedsize"] / 1048576)
            elif diskusage["usedsize"] / 1024 >= 1:
                #more than 1M
                diskusage["usedsize"] = "{:,.2f}M".format(diskusage["usedsize"] / 1024)
            else:
                diskusage["usedsize"] = "{:,}K".format(diskusage["usedsize"])

        return diskusage

    def manage(self,clean_interval=864000,check_increments=1048576,max_cleantime=0,clean_threshold=0.8,emergencyclean_threshold=0.9):
        """
        Manage the cache of  gwc layers with the following steps
        1. Execute the uncompleted clean batch if have
        2. Start a new clean batch if the used percentage reachs the threshold or the clean interval is passed
        3. Start the emergency clean if the used percentage reachs the emergency clean threshold
        4. Find the tiles size of each gwc layers if some clean job is executed or the increment of the overal tiles size reachs the threshold

        Return a tuple ((cleaned?, released storage),(checked?,gwc disk size,total tiles size,used percentage ))
        """
        self.load_cleaning_status()

        #sort the layers
        self._layers.sort(key=lambda o:o["name"])

        starttime = timezone.localtime()

        cleaned = False
        checked = False

        diskinfo_after_clean = None
        diskinfo_before_clean = self.get_diskinfo()
        try:
            #finish the uncompleted clean batch first.
            if self._managementstatus.get("cleanbatchid") and not self._managementstatus.get("clean_succeed",False):
                #has a uncompleted clean batch, finished it first
                lastcleanbatchid = self._managementstatus["cleanbatchid"]
                cleaned = True
                for layer in self._layers:
                    if layer[self.KEY_MANAGEMENTSTATUS].get("cleanbatchid") == lastcleanbatchid and layer[self.KEY_MANAGEMENTSTATUS].get("clean_succeed",False):
                        #already cleaned before
                        continue
    
                    layer[self.KEY_MANAGEMENTSTATUS]["cleanbatchid"] = lastcleanbatchid
                    self.clean_layer_cache(layer)
                    if max_cleantime > 0 and (timezone.localtime() - starttime).total_seconds() >= max_cleantime:
                        #done
                        raise RunOutofTimeException()
                self._managementstatus["clean_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
    
            #normal clean if required
            cleanbatchid = None
            clean_starttime = self._managementstatus.get("clean_starttime")
            if clean_starttime:
                clean_starttime = timezone.parse(clean_starttime,"%Y-%m-%d %H:%M:%S")

            if not self._managementstatus.get("cleanbatchid") or not clean_starttime or (starttime.date() - clean_starttime.date()).days >= clean_interval:
                #not clean before or reach the clean interval
                cleanbatchid = timezone.format(starttime,pattern="%Y-%m-%d %H:%M:%S")
            else:
                if diskinfo_before_clean and diskinfo_before_clean[0] > 0 and diskinfo_before_clean[1] / diskinfo_before_clean[0] >= clean_threshold:
                    #reach the clean_threshold.
                    cleanbatchid = timezone.format(starttime,pattern="%Y-%m-%d %H:%M:%S")
    
            if cleanbatchid:
                #start the normal clean
                cleaned = True
                self._managementstatus["cleanbatchid"] = cleanbatchid
                self._managementstatus["clean_starttime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
                self._managementstatus["clean_succeed"] = True
                self._managementstatus["clean_emergency"] = False
                self._managementstatus["clean_message"] = "Succeed"

                for layer in self._layers:
                    if  layer[self.KEY_MANAGEMENTSTATUS].get("clean_starttime") and layer[self.KEY_MANAGEMENTSTATUS].get("clean_starttime") >= cleanbatchid:
                        #already cleaned in previous step, no need to clean it again
                        layer[self.KEY_MANAGEMENTSTATUS]["cleanbatchid"] = cleanbatchid
                        continue

                    layer[self.KEY_MANAGEMENTSTATUS]["cleanbatchid"] = cleanbatchid
                    self.clean_layer_cache(layer)
                    if max_cleantime > 0 and (timezone.localtime() - starttime).total_seconds() > max_cleantime:
                        #done
                        raise RunOutofTimeException()
                self._managementstatus["clean_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")

            #emergency clean if required
            nomore_cleaning = True
            cleanbatchid = None
            cleanround = 1
            layerindex = 0
            emergency_starttime = timezone.localtime()
            while True:
                diskinfo_before_emergencyclean = self.get_diskinfo()
    
                if not diskinfo_before_emergencyclean or diskinfo_before_emergencyclean[0] <= 0 or diskinfo_before_emergencyclean[1] / diskinfo_before_emergencyclean[0] < emergencyclean_threshold:
                    #not reach the emergency threshold
                    break
                
                cleaned = True
                if not cleanbatchid:
                    cleanbatchid = timezone.format(emergency_starttime,pattern="%Y-%m-%d %H:%M:%S")
                    self._managementstatus["cleanbatchid"] = cleanbatchid
                    self._managementstatus["clean_starttime"] = timezone.format(starttime,pattern="%Y-%m-%d %H:%M:%S")
                    self._managementstatus["clean_succeed"] = True
                    self._managementstatus["clean_emergency"] = True
                    self._managementstatus["clean_message"] = "Succeed"
    
                while layerindex < len(self._layers):
                    if self.clean_layer_cache(self._layers[layerindex],True,cleanround=cleanround):
                        #some older tiles were cleaned,
                        #can continue to clean more older tiles if necessary
                        nomore_cleaning = False
                        layerindex += 1
                        break
                    else:
                        layerindex += 1
    
                if layerindex == len(self._layers):
                    #already Finish one round of emergency cleaning
                    #start the another round emergency cleaning until disk usage is lower than the threadhold
                    if nomore_cleaning:
                        #already deleted all possible tiles,
                        logger.debug("No more tiles can be delete in the next round of emergency cleaning. stop emergency cleaning.")
                        break
                    elif cleanround < 9:
                        layerindex = 0
                        nomore_cleaning = True
                        logger.debug("Already cleaned {0}0%(time based) of tiles from all gwc layers. begin to start the {1}th round of cleaning.".format(cleanround,cleanround))
                        cleanround += 1
                    else:
                        logger.debug("Already cleaned {}0%(time based) of tiles from all gwc layers. stop emergency cleaning.".format(cleanround))
                        break

            self._managementstatus["clean_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")

            #find the tiles_totalsize for  each layer if cleaned or not checked before or tiles_totalsize reach the check increments
            diskinfo_after_clean = self.get_diskinfo() if cleaned else diskinfo_before_clean
            if cleaned or not self._managementstatus.get("tiles_totalsize") or (diskinfo_after_clean and abs(diskinfo_after_clean[1] - self._managementstatus["tiles_totalsize"]) >= check_increments):
                checked = True
                self._managementstatus["check_starttime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
                self._managementstatus["check_succeed"] = True
                self._managementstatus["check_message"] = "Succeed"
                try:
                    for layer in self._layers:
                        layer_tile_dir = "{}/{}_{}".format(self.gwc_tiles_dir,layer["name"][0],layer["name"][1])
                        managementstatus = layer[self.KEY_MANAGEMENTSTATUS]
                        managementstatus["check_starttime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
                        if os.path.exists(layer_tile_dir):
                            cmd = "du -k -d 0 \"{}\"".format(layer_tile_dir)
                            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
                            if result.returncode != 0:
                                managementstatus["check_message"] = "Failed to get the cache size for layer({}:{}).{}".format(layer["name"][0],layer["name"][1],result.stderr)
                            else:
                                managementstatus["tiles_totalsize"] = int(result.stdout.split()[0])
                        else:
                            managementstatus["tiles_totalsize"] = 0
                        managementstatus["check_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
                except Exception as ex:
                    self._managementstatus["check_message"] = "Failed to get the cache size for layer({}:{}).{}".format(layer["name"][0],layer["name"][1],traceback.format_exc())
                    self._managementstatus["check_succeed"] = False
                finally:
                    self._managementstatus["check_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
        except Exception as ex:
            if cleaned and not checked:
                #exceptions happened during cleaning
                self._managementstatus["clean_endtime"] = timezone.format(timezone.localtime(),pattern="%Y-%m-%d %H:%M:%S")
                self._managementstatus["clean_succeed"] = False
                if isinstance(ex,RunOutofTimeException):
                    self._managementstatus["clean_message"] = "End the clean process because of running out of time."
                else:
                    self._managementstatus["clean_message"] = traceback.format_exc()
        finally:
            if cleaned:
                if not diskinfo_after_clean:
                    diskinfo_after_clean = self.get_diskinfo()
                if diskinfo_after_clean:
                    self._managementstatus["tiles_totalsize"] = diskinfo_after_clean[1]
                    if diskinfo_before_clean:
                        self._managementstatus["released_diskspace"] = diskinfo_before_clean[1] - diskinfo_after_clean[1]


        if not diskinfo_after_clean:
            diskinfo_after_clean = self.get_diskinfo() if cleaned else diskinfo_before_clean

        if checked or cleaned:
            #save the managementstatus
            gwcmanagementstatus = {self.KEY_MANAGEMENTSTATUS:self._managementstatus}
            for layer in self._layers:
                if not layer[self.KEY_MANAGEMENTSTATUS]:
                    continue
                managementstatus = layer[self.KEY_MANAGEMENTSTATUS]
                if layer["name"][0] not in gwcmanagementstatus:
                    gwcmanagementstatus[layer["name"][0]] = {}
                gwcmanagementstatus[layer["name"][0]][layer["name"][1]] = managementstatus

            #save gwcmanagementstatus
            try:
                with open(self.gwcmanagementstatusfile,"w") as f:
                    if settings.DEBUG:
                        f.write(json.dumps(gwcmanagementstatus,indent=4))
                    else:
                        f.write(json.dumps(gwcmanagementstatus))
            except Exception as ex:
                logger.error("Failed to save layer clean status file '{}'.{}".format(self.gwcmanagementstatusfile,str(ex)))

            #populate the gwclayers.html
            for layer in self._layers:
                if not layer[self.KEY_MANAGEMENTSTATUS]:
                    continue
                managementstatus = layer[self.KEY_MANAGEMENTSTATUS]
                if "tiles_totalsize" not in managementstatus:
                    managementstatus["tiles_totalsize_human"] = "?"
                elif managementstatus.get("tiles_totalsize") / 1048576 > 1:
                    #more than 1G
                    managementstatus["tiles_totalsize_human"] = "{:,.2f}G".format(managementstatus["tiles_totalsize"] / 1048576)
                elif managementstatus.get("tiles_totalsize") / 1024 > 1:
                    #more than 1M
                    managementstatus["tiles_totalsize_human"] = "{:,.2f}M".format(managementstatus["tiles_totalsize"] / 1024)
                else:
                    managementstatus["tiles_totalsize_human"] = "{:,}K".format(managementstatus["tiles_totalsize"])

                layer["expireCache_human"] = utils.format_timedelta(layer.get("expireCache",0),'s')


            if self._managementstatus["clean_succeed"]:
                if "released_diskspace" not in self._managementstatus:
                    self._managementstatus["clean_message"] = "Successfully cleaned the gwc cache, but don't know the exact disk space released during {}cleaning.".format("emergency " if self._managementstatus.get("clean_emergency") else "")
                elif  self._managementstatus["released_diskspace"] / 1048576 > 1:
                    #more than 1G
                    self._managementstatus["clean_message"] = "Successfully cleaned the gwc cache, release {:,.2f}G disk space during {}cleaning.".format(self._managementstatus["released_diskspace"] / 1048576,"emergency " if self._managementstatus.get("clean_emergency") else "")
                elif self._managementstatus["released_diskspace"] / 1024 > 1:
                    #more than 1M
                    self._managementstatus["clean_message"] = "Successfully cleaned the gwc cache, release {:,.2f}M disk space during {}cleaning.".format(self._managementstatus["released_diskspace"] / 1024,"emergency " if self._managementstatus["clean_emergency"] else "")
                else:
                    self._managementstatus["clean_message"] = "Successfully cleaned the gwc cache, release {:,}K disk space during {}cleaning.".format(self._managementstatus["released_diskspace"],"emergency " if self._managementstatus.get("clean_emergency") else "")

            diskusage = self.get_diskusagedata(diskinfo_after_clean)

            #sort the layers on cache_size
            self._layers.sort(key=lambda o:o.get(self.KEY_MANAGEMENTSTATUS).get("tiles_totalsize",0),reverse=True)
            #generate the gwclayers.html
            reports_template = jinja_env.get_template("gwclayers.html")
            with open(self.gwclayersfile,"w") as f:
                f.write(reports_template.render({
                    "geoserver" : self.geoserver_name,
                    "diskusage" : diskusage,
                    "layers":self._layers,
                    "managementstatus": self._managementstatus
                }))
            logger.debug("Succeed to populate the gwclayers.html({})".format(self.gwclayersfile))
        if diskinfo_after_clean:
            check_data = (checked,diskinfo_after_clean[0],diskinfo_after_clean[1],diskinfo_after_clean[1] / diskinfo_after_clean[0] if diskinfo_after_clean[0] else 'N/A')
        else:
            check_data = (checked,None,None,None)
        if cleaned:
            if diskinfo_after_clean and diskinfo_before_clean:
                return ((True,diskinfo_after_clean[1] - diskinfo_before_clean[1]),check_data)
            else:
                return ((True,-1),check_data)
        else:
            return ((False,None),check_data)

