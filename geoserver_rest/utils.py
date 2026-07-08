import re
import traceback
import os

from datetime import timedelta


def has_samedata(datas1,datas2):
    """

    Return True if all the data in datas1 are in datas2 and all the data in datas2 and in datas1, ignore the order

    """
    if not datas1 and not datas2:
        return True
    elif not datas1:
        return False
    elif not datas2:
        return False
    elif len(datas1) != len(datas2):
        return False
    else:
        return False if any(d for d in datas1 if d not  in datas2) else True

def is_contain(datas,subdatas):
    """
    Return true if all datas in subdatas are in datas.
    """
    if not datas and not subdatas:
        return True
    elif not datas:
        return False
    elif not subdatas:
        return True
    elif len(datas) < len(subdatas):
        return False
    else:
        return False if any(d for d in subdatas if d not  in datas) else True

    
domain_url_re = re.compile("^((?P<protocol>[a-z]+)://)?(?P<domain>[^:/\\?#]+)",re.IGNORECASE)
def get_domain(url):
    """
    Return domain from url
    """
    if url:
        m = domain_url_re.search(url)
        if m :
            return m.group('domain')
        else:
            return None
    else:
        return None

def toMapKey(d):
    if d is None:
        return d
    elif isinstance(d,(str,bool,int,float)):
        return d
    elif isinstance(d,set):
        data = list(d)
        data.sort()
        return toMapKey(data)
    elif isinstance(d,tuple):
        return tuple(toMapKey(o) for o in d)
    elif isinstance(d,list):
        return tuple([toMapKey(o) for o in d])
    elif isinstance(d,dict):
        keys = [k for k in d.keys()]
        keys.sort()
        return tuple([ (k,toMapKey(d[k])) for k in keys])
    else:
        try:
            return tuple([toMapKey(o) for o in iter(d)])
        except:
            return d

def get_iter(coordinates):
    if not coordinates:
        return
    if all(isinstance(c,(int,float)) for c in coordinates):
        yield coordinates
        return
    for coord in coordinates:
        if all(isinstance(c,(int,float)) for c in coord):
            yield coord
        else:
            for c in get_iter(coord):
                yield c

def get_bbox(coordinates):
    bbox = [None,None,None,None]
    for coord in get_iter(coordinates):
        if not coord:
            continue

        if coord[0] is not None:
            if not bbox[0] or bbox[0] > coord[0]:
                bbox[0] = coord[0]
            if not bbox[2] or bbox[2] < coord[0]:
                bbox[2] = coord[0]

        if coord[1] is not None:
            if not bbox[1] or bbox[1] > coord[1]:
                bbox[1] = coord[1]
            if not bbox[3] or bbox[3] < coord[1]:
                bbox[3] = coord[1]

    return None if any(c is None for c in bbox) else bbox

def remove_file(f):
    if not f:
        return
    
    try:
        os.remove(f)
    except Exception as ex:
        pass
                
def format_timedelta(td,unit="s"):
    days = 0
    hours = 0
    minutes = 0
    seconds = 0
    if isinstance(td,timedelta):
        days = td.days
        seconds = td.seconds
        hours = int(seconds / (60 * 60))
        seconds = seconds % (60 * 60)
        minutes = int(seconds / 60)
        seconds = seconds % 60
    else:
        if unit == "d":
            days = td
        elif unit == "h":
            days = int(td / 24)
            hours = td % 24
        elif unit == "m":
            days = int(td / (24 * 60))
            minutes = td % (24 * 60)
            hours = int(minutes / 60)
            minutes = minutes % 60
        elif unit == "s":
            days = int(td / (24 * 60 * 60))
            seconds = td % (24 * 60 * 60)
            hours = int(seconds / (60 * 60))
            seconds = seconds % (60 * 60)
            minutes = int(seconds / 60)
            seconds = seconds % 60


    days = "" if days == 0 else "{}D".format(days)
    hours = "" if hours == 0 else "{}H".format(hours)
    minutes = "" if minutes == 0 else "{}M".format(minutes)
    seconds = "" if seconds == 0 else "{}S".format(seconds)

    return "".join(d for d in [days,hours,minutes,seconds] if d)
                

