"""
Main application entry point for the distributed search worker node.

This module connects to ZooKeeper, manages leader election, service registration,
and runs the worker node's main loop.

Classes:
- Application: Manages ZooKeeper connection, leader election, and service lifecycle.

Usage:
Run this script with an optional port argument to start the worker node.
"""
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
import sys
import threading
import asyncio 
import time

from cluster.management.service_registry import ServiceRegistry
from on_election_action import OnElectionAction
from leader_election import LeaderElection


class Application:
    """
    Application class to manage ZooKeeper connection, leader election,
    service registration, and main event loop.
    """

    ZOOKEEPER_ADDRESS = "127.0.0.1:2181"
    SESSION_TIMEOUT = 3.0  # seconds

    def __init__(self, port=8080):
        """
        Initialize the Application.

        :param port: Port number for the worker node's HTTP server.
        """
        self.current_server_port = port
        self.zk = None
        self.leader_election = None
        self.service_registry = None
        self.on_election_action = None
        self.connected_event = threading.Event()

    def connect_to_zookeeper(self):
        """
        Connect to ZooKeeper server and wait for connection.

        :return: KazooClient instance connected to ZooKeeper.
        """
        self.zk = KazooClient(hosts=self.ZOOKEEPER_ADDRESS, timeout=self.SESSION_TIMEOUT)
        self.zk.add_listener(self.zk_listener)
        self.zk.start()
        self.connected_event.wait()
        return self.zk

    def zk_listener(self, state):
        """
        ZooKeeper connection state listener.

        :param state: Connection state string.
        """
        if state == "CONNECTED":
            print("Successfully connected to Zookeeper")
            self.connected_event.set()
        elif state == "LOST":
            print("Disconnected from Zookeeper")
            self.connected_event.clear()

    def run(self):
        """
        Main event loop. Keeps the application running until interrupted.
        """
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Interrupted, shutting down")

    def close(self):
        """
        Close the ZooKeeper connection.
        """
        if self.zk:
            self.zk.stop()
            self.zk.close()

    def main(self):
        """
        Main method to start the application:
        - Connect to ZooKeeper
        - Initialize service registry and election callbacks
        - Volunteer for leadership and start leader election
        - Run main event loop
        - Close connection on exit
        """
        zk = self.connect_to_zookeeper()
        self.service_registry = ServiceRegistry(zk, ServiceRegistry.WORKERS_REGISTRY_ZNODE)
        self.on_election_action = OnElectionAction(self.service_registry, self.current_server_port)
        self.leader_election = LeaderElection(zk, self.on_election_action)

        self.leader_election.volunteer_for_leadership()
        self.leader_election.reelect_leader()

        self.run()
        self.close()
        print("Disconnected from Zookeeper, exiting application")


if __name__ == "__main__":
    port = 8080
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    app = Application(port)
    app.main()

