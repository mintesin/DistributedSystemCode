"""
Implements leader election using Kazoo (Python ZooKeeper client)

This class manages the leader election process by creating ephemeral sequential znodes
under the /election namespace. The node with the smallest znode name is the leader.
Other nodes watch their predecessor znode for deletion to trigger re-election.

Methods:
- volunteer_for_leadership: Creates an ephemeral sequential znode to volunteer for leadership.
- reelect_leader: Determines if the current node is the leader or sets a watch on the predecessor.
- process: Placeholder for event processing (Kazoo uses DataWatch and StateListener instead).
"""

from kazoo.client import KazooClient
import threading


class LeaderElection:
    ELECTION_NAMESPACE = "/election"

    def __init__(self, zk: KazooClient, on_election_callback):
        """
        Initialize LeaderElection with a KazooClient instance and a callback handler.

        :param zk: KazooClient instance connected to ZooKeeper.
        :param on_election_callback: Callback object implementing on_elected_to_be_leader and on_worker methods.
        """
        self.zk = zk
        self.on_election_callback = on_election_callback
        self.current_znode_name = None
        self.lock = threading.Lock()

    def volunteer_for_leadership(self):
        """
        Volunteer for leadership by creating an ephemeral sequential znode under the election namespace.
        Stores the created znode name for leader election comparison.
        """
        znode_prefix = self.ELECTION_NAMESPACE + "/c_"
        znode_full_path = self.zk.create(znode_prefix, b"", ephemeral=True, sequence=True)
        print(f"znode name {znode_full_path}")
        self.current_znode_name = znode_full_path.replace(self.ELECTION_NAMESPACE + "/", "")

    def reelect_leader(self):
        """
        Perform leader election by checking the smallest znode.
        If current node is the smallest, it becomes the leader and triggers the leader callback.
        Otherwise, it sets a watch on its predecessor znode to detect leader changes.
        """
        with self.lock:
            children = self.zk.get_children(self.ELECTION_NAMESPACE)
            children.sort()
            smallest_child = children[0]

            if smallest_child == self.current_znode_name:
                print("I am the leader")
                self.on_election_callback.on_elected_to_be_leader()
            else:
                print("I am not the leader")
                current_index = children.index(self.current_znode_name)
                predecessor_index = current_index - 1
                predecessor_znode_name = children[predecessor_index]

                @self.zk.DataWatch(self.ELECTION_NAMESPACE + "/" + predecessor_znode_name)
                def watch_node(data, stat, event):
                    if event and event.type == "DELETED":
                        print(f"Watched znode {predecessor_znode_name} deleted, reelecting leader")
                        self.reelect_leader()
                        return False  # Stop watching

                self.on_election_callback.on_worker()
                print(f"Watching znode {predecessor_znode_name}")

    def process(self, event):
        """
        Placeholder for event processing.
        Kazoo uses DataWatch and StateListener instead of this method.
        """
        pass
