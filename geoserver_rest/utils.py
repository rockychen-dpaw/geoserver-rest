import re

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

