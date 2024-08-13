import requests

class UnauthorizedException(requests.RequestException):
    def __init__(self,response):
        super().__init__("Not Authorized",response=response)

class ResourceNotFound(requests.RequestException):
    def __init__(self,response):
        super().__init__("Resource Not Found",response=response)

class GetMapFailed(requests.RequestException):
    def __init__(self,response):
        super().__init__("Failed to get the map",response=response)
