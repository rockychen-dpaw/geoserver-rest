import requests

class UnauthorizedException(requests.RequestException):
    def __init__(self,response):
        super().__init__("Not Authorized",response=response)

class ResourceNotFound(requests.RequestException):
    def __init__(self,response):
        msg = "Resource({0}) Not Found".format(response.request.url)
        super().__init__(msg,response=response)

class GetMapFailed(requests.RequestException):
    def __init__(self,msg,response):
        super().__init__(msg,response=response)
