from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import KazooException, NoNodeError 
import time
import threading 


class LeaderElection:
    def __init__(self):
        pass