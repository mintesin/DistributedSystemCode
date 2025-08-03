"""
Implements OnElectionCallback interface with leader and worker actions
"""

import socket
from kazoo.exceptions import KazooException
from cluster.management.service_registry import ServiceRegistry
from networking.web_server import WebServer
from search.search_worker import SearchWorker


class OnElectionAction:
    def __init__(self, service_registry: ServiceRegistry, port: int):
        """
        Initialize the OnElectionAction.
        :param service_registry: ServiceRegistry instance for cluster registration.
        :param port: Port number for the worker's HTTP server.
        """
        self.service_registry = service_registry
        self.port = port
        self.web_server = None  # Will hold the web server instance for worker

    def on_elected_to_be_leader(self):
        """
        Actions to perform when this node is elected as leader:
        - Unregister from the worker cluster (no longer a worker)
        - Register for updates (to monitor cluster changes)
        """
        self.service_registry.unregister_from_cluster()
        self.service_registry.register_for_updates()

    def on_worker(self):
        """
        Actions to perform when this node is a worker:
        - Start the web server if not already started
        - Register this worker's address in the service registry
        """
        search_worker = SearchWorker()
        if self.web_server is None:
            self.web_server = WebServer(self.port, search_worker)
            self.web_server.start_server()

        try:
            hostname = socket.gethostname()
            current_server_address = f"http://{hostname}:{self.port}{search_worker.get_endpoint()}"
            self.service_registry.register_to_cluster(current_server_address)
        except Exception as e:
            print(f"Error registering to cluster: {e}")
