from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


def get_traversal(endpoint):
    """Given gremlin endpoint get connected remote traversal."""
    return traversal().withRemote(
        DriverRemoteConnection(endpoint, "g")
    )
