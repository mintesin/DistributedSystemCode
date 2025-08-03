from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import KazooException, NoNodeError 
import time
import threading
# if elected as leader unregsiter from clusterand get info from 
# service registry about aother working nodes as a leader
# if elelcted as a worker create a webserver, run the work load and respind to the client or to the leader

class OnelectionCallBack:
    def __init__(self):
        pass