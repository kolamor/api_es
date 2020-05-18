import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import Master, RejectMessage, NackMessage


class Publisher:
    _connection = None

    def __init__(self, routing_key, name):
        self.routing_key = routing_key
        self.name = name

    @classmethod
    async def create(cls, routing_key, uri, name):
        self = cls(routing_key, name=name)
        connection = await connect_robust(uri)
        self._connection = connection
        return self

    async def start(self):

        # Creating channel
        channel = await self._connection.channel()

        master = Master(channel)

        # # Creates tasks by proxy object
        # for task_id in range(1000):
        #     await master.proxy.my_task_name(task_id=task_id)

        # Or using create_task method
        for task_id in range(3):
            await master.create_task(
                "my_task_name", kwargs=dict(task_id=task_id)
            )


class Worker:
    _connection = None

    def __init__(self, routing_key, name):
        self.routing_key = routing_key
        self.name = name

    @classmethod
    async def create(cls, routing_key, uri, name):
        self = cls(routing_key, name)
        connection = await connect_robust(uri)

        self._connection = connection
        return self

    async def start(self):
        # Creating channel
        channel = await self._connection.channel()
        master = Master(channel)
        await master.create_worker("my_task_name", self.worker, auto_delete=True)

    async def worker(self, *, task_id):
        # If you want to reject message or send
        # nack you might raise special exception

        # if task_id % 2 == 0:
        #     raise RejectMessage(requeue=False)
        #
        # if task_id % 2 == 1:
        #     raise NackMessage(requeue=False)

        print(task_id)
        await asyncio.sleep(0)


async def start_processed(app):
    app['rabbit_connections'] = []
    publisher = await Publisher.create('1_1', uri=app['config']['connection_rmq_uri'], name='publisher_1')
    app['rabbit_connections'].append(publisher)
    asyncio.create_task(publisher.start())

    for n in range(1, 4):
        worker = await Worker.create('1_1', uri=app['config']['connection_rmq_uri'], name=f'worker_{n}')
        app['rabbit_connections'].append(worker)
        asyncio.create_task(worker.start())
        await asyncio.sleep(0)
