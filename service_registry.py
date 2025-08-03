"""
ServiceRegistry module manages service registration and discovery using Kazoo (Python ZooKeeper client).

This class handles registering services as ephemeral sequential znodes under a specified znode path,
unregistering services, and watching for updates to the list of registered services.

Class:
- ServiceRegistry: Manages service registration and discovery.

Usage:
Create an instance with a KazooClient and a registry znode path, then use register_to_cluster,
unregister_from_cluster, and register_for_updates methods to manage service lifecycle.
"""

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from kazoo.protocol.states import WatchedEvent, EventType
import threading


class ServiceRegistry:
    """
    ServiceRegistry manages service registration and discovery in ZooKeeper.

    Attributes:
    - WORKERS_REGISTRY_ZNODE: Znode path for worker service registry.
    - COORDINATORS_REGISTRY_ZNODE: Znode path for coordinator service registry.
    """

    WORKERS_REGISTRY_ZNODE = "/workers_service_registry"
    COORDINATORS_REGISTRY_ZNODE = "/coordinators_service_registry"

    def __init__(self, zk: KazooClient, service_registry_znode: str):
        """
        Initialize ServiceRegistry.

        :param zk: KazooClient instance connected to ZooKeeper.
        :param service_registry_znode: Znode path for service registry.
        """
        self.zk = zk
        self.service_registry_znode = service_registry_znode
        self.all_service_addresses = None
        self.current_znode = None
        self.lock = threading.Lock()
        self.create_service_registry_node()

    def create_service_registry_node(self):
        """
        Create the service registry znode if it does not exist.
        """
        if not self.zk.exists(self.service_registry_znode):
            self.zk.create(self.service_registry_znode, b"", makepath=True)

    def register_to_cluster(self, metadata: str):
        """
        Register the service to the cluster by creating an ephemeral sequential znode with metadata.

        :param metadata: Metadata string to store in the znode.
        """
        if self.current_znode is not None:
            print("Already registered to service registry")
            return
        self.current_znode = self.zk.create(
            self.service_registry_znode + "/n_",
            metadata.encode(),
            ephemeral=True,
            sequence=True,
        )
        print("Registered to service registry")

    def unregister_from_cluster(self):
        """
        Unregister the service from the cluster by deleting its znode.
        """
        if self.current_znode and self.zk.exists(self.current_znode):
            self.zk.delete(self.current_znode)
            self.current_znode = None

    def register_for_updates(self):
        """
        Register for updates to the service registry by watching the registry znode.
        """
        try:
            self.update_addresses()
        except KazooException:
            pass

    def get_all_service_addresses(self):
        """
        Get the list of all registered service addresses.

        :return: Tuple of service addresses.
        """
        with self.lock:
            if self.all_service_addresses is None:
                self.update_addresses()
            return self.all_service_addresses

    def update_addresses(self):
        """
        Update the list of service addresses by reading children znodes and their data.
        Sets a watch on the registry znode for changes.
        """
        with self.lock:
            children = self.zk.get_children(self.service_registry_znode, watch=self.process)
            addresses = []
            for child in children:
                service_full_path = self.service_registry_znode + "/" + child
                if self.zk.exists(service_full_path):
                    data, _ = self.zk.get(service_full_path)
                    addresses.append(data.decode())
            self.all_service_addresses = tuple(addresses)
            print(f"The cluster addresses are: {self.all_service_addresses}")

    def process(self, event: WatchedEvent):
        """
        Watcher callback to handle ZooKeeper events.

        :param event: WatchedEvent object.
        """
        if event.type == EventType.CHILD:
            try:
                self.update_addresses()
            except KazooException:
                pass
