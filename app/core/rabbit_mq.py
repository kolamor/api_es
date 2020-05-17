

class Publisher:

    def __init__(self, connection, routing_key):
        self._connection = connection
        self.routing_key = routing_key

    @classmethod
    async def create(cls, connection, routing_key):
        self = cls(connection, routing_key)
        return self
