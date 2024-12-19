from cassandra.cluster import Cluster


def create_cassandra_connection():
    """Create and return a Cassandra connection."""
    cluster = Cluster(["cassandra"])  # Replace with your Cassandra instance's IP
    session = cluster.connect()
    session.set_keyspace("driver_data")  # Replace with your keyspace name
    return session
