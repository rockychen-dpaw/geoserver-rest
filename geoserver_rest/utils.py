def encode_xmltext(text):
    if not text:
        return ""
    result = None
    for i in range(len(text)):
        if text[i] not in ['&'] and text[i] in string.printable:
            if result :
                result += text[i]
        else:
            if result is None:
                result = text[:i]
            result += "&#{};".format(ord(text[i]))

    return result if result else text

def contenttype_header(f = "xml"):
    if f == "xml":
        return {"content-type": "application/xml"}
    elif f == "json":
        return {"content-type": "application/json"}
    else:
        raise Exception("Format({}) Not Support".format(f))

def accept_header(f = "xml"):
    if f == "xml":
        return {"Accept": "application/xml"}
    elif f == "json":
        return {"Accept": "application/json"}
    elif f == "html":
        return {"Accept": "text/html"}
    else:
        raise Exception("Format({}) Not Support".format(f))

