import os
from aiogremlin import DriverRemoteConnection

CONNECTION_RETRIES = 5
CONNECTION_HEARTBEAT = 0.1

class NeptuneConnectionPool():
    def __init__(self, users):
        self.users = users
        self.active = []
        self.available = []

    async def create(self):
        for _ in range(self.users):
            conn = await self.init_neptune_connection()
            self.available.append(conn)

    async def destroy(self):
        for conn in self.active + self.available:
            await conn.close()

    def lock(self):
        for _ in range(CONNECTION_RETRIES):
            if self.available:
                conn = self.available.pop()
                self.active.append(conn)
                return conn
        raise ConnectionError("Cannot aquire connection from pool.")

    def unlock(self, conn):
        self.active.remove(conn)
        self.available.append(conn)

    async def init_neptune_connection(self):
        """Init Neptune connection."""
        endpoint = os.environ["NEPTUNE_CLUSTER_ENDPOINT"]
        port = os.getenv("NEPTUNE_CLUSTER_PORT", "8182")
        return await DriverRemoteConnection.open(f"ws://{endpoint}:{port}/gremlin", "g")
