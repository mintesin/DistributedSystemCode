from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
import time

# ---------------------------- #
#     Zookeeper Config        #
# ---------------------------- #

# Zookeeper server address (use actual address in production)
ZOOKEEPER_ADDRESS = 'localhost:2181'

# Path in Zookeeper used to store election znodes
ELECTION_NAMESPACE = '/election'

# Timeout for Zookeeper session in seconds
SESSION_TIMEOUT = 3

# ---------------------------- #
#     ElectionLeader Class     #
# ---------------------------- #

class ElectionLeader:
    """
    This class handles the leader election process using Apache Zookeeper.
    Each instance of the class connects to Zookeeper, creates an ephemeral 
    sequential znode in the election path, and determines whether it is 
    the leader based on the sequence number.
    """

    def __init__(self):
        """
        Initialize the ElectionLeader instance and prepare the Zookeeper client.
        """
        # Create a Kazoo client for Zookeeper connection
        self.zookeeper = KazooClient(hosts=ZOOKEEPER_ADDRESS, timeout=SESSION_TIMEOUT)
        self.current_znode_name = None  # Stores the name of this node's znode

    def connect_to_zookeeper(self):
        """
        Connects to the Zookeeper ensemble.
        """
        self.zookeeper.start()
        print("✅ Successfully connected to Zookeeper")

    def create_election_znode(self):
        """
        Ensures that the election namespace exists in Zookeeper.
        If it doesn't exist, creates it.
        """
        if not self.zookeeper.exists(ELECTION_NAMESPACE):
            try:
                self.zookeeper.create(ELECTION_NAMESPACE)
                print("📁 Created election namespace in Zookeeper")
            except NodeExistsError:
                # Node already exists; safe to ignore
                pass

    def volunteer_for_leadership(self):
        """
        Creates an ephemeral sequential znode under the election namespace.
        This znode represents the current instance's candidacy for leadership.
        """
        # Construct the znode prefix (Zookeeper appends a sequence number)
        znode_prefix = ELECTION_NAMESPACE + '/n_'

        # Create an ephemeral sequential node (auto-appends a sequence number)
        self.current_znode_name = self.zookeeper.create(znode_prefix, ephemeral=True, sequence=True)

        # Extract only the node name (e.g., 'n_0000000031')
        self.current_znode_name = self.current_znode_name.split("/")[-1]

        print(f"📌 Volunteered for leadership with znode: {self.current_znode_name}")

    def elect_leader(self):
        """
        Determines if the current instance is the leader by comparing its znode 
        name with the lowest (oldest) znode name in the election namespace.
        """
        try:
            # Get all child znodes under the election path
            children = self.zookeeper.get_children(ELECTION_NAMESPACE)

            # Sort the children to find the one with the smallest sequence number
            children.sort()

            # The smallest child is the leader
            smallest_child = children[0]

            if smallest_child == self.current_znode_name:
                print("👑 I am the leader")
            else:
                print(f"🙇 I am not the leader. The current leader is: {smallest_child}")
        except NoNodeError:
            # The election node doesn't exist (shouldn't happen if properly initialized)
            print("❌ Election node does not exist")

    def run(self):
        """
        Keeps the program running to maintain the ephemeral znode.
        Use Ctrl+C to stop the program.
        """
        try:
            print("🏃 Running... Press Ctrl+C to stop.")
            while True:
                time.sleep(1)  # Keeps the session alive
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")

    def close(self):
        """
        Gracefully shuts down the Zookeeper client connection.
        This also deletes the ephemeral znode automatically.
        """
        self.zookeeper.stop()
        self.zookeeper.close()
        print("🔌 Disconnected from Zookeeper. Application is down.")

# ---------------------------- #
#         Entry Point          #
# ---------------------------- #

if __name__ == "__main__":
    # Create an instance of the ElectionLeader
    leader_election = ElectionLeader()

    try:
        # Step 1: Connect to Zookeeper
        leader_election.connect_to_zookeeper()

        # Step 2: Create the election namespace if it doesn't exist
        leader_election.create_election_znode()

        # Step 3: Volunteer to become the leader
        leader_election.volunteer_for_leadership()

        # Step 4: Determine who the leader is
        leader_election.elect_leader()

        # Step 5: Keep the application running to maintain leadership (or observe leadership change)
        leader_election.run()

    finally:
        # Ensure Zookeeper connection is always closed properly
        leader_election.close()
