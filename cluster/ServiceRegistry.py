from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import KazooException, NoNodeError 
import time
import threading
# rcheckn and create node election registry
# method for registering for registry
#method for unregistreing from the service
class ServiceRegistry:
    def __init__(self):
        pass