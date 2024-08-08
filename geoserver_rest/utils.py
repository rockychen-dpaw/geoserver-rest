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

    
