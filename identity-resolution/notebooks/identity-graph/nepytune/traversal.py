from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.aiohttp.transport import AiohttpTransport

def get_traversal(endpoint):
    """Given gremlin endpoint get connected remote traversal."""
    return traversal().withRemote(
        DriverRemoteConnection(endpoint, "g",
          transport_factory=lambda:AiohttpTransport(call_from_event_loop=True))
    )