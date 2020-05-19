import asyncio
from uuid import uuid4
import logging
import functools
from aio_pika import connect_robust
from aio_pika.patterns import Master, RejectMessage, NackMessage
from inspect import isgenerator
from ..core.es_query import EsQuery


logger = logging.getLogger(__name__)


class Publisher:
    _connection = None  # aio_pika.RobustConnection
    _channel = None  # aio_pika.RobustChannel
    _master = None  # aio_pika.patterns.Master
    _queue = None  # asyncio.Queue
    __gen_task_id = None  # generator

    def __init__(self, routing_key, name):
        self.routing_key = routing_key
        self.name = name
        self._uuid = uuid4().hex
        self.__is_run = False

    @classmethod
    async def create(cls, routing_key, uri, name):
        self = cls(routing_key, name=name)
        await self.__create(uri)
        return self

    async def __create(self, uri):
        self._connection = await connect_robust(uri)
        # Creating channel
        self._channel = await self._connection.channel()
        self._master = Master(self._channel)

    def get_task_id(self) -> str:
        """
        create generator and(or) to iterables,
        :return: str
        """
        def gen():
            n = 0
            while True:
                task_id = f'{self._uuid}_{n}'
                yield task_id
                n += 1

        if not isgenerator(self.__gen_task_id):
            self.__gen_task_id = gen()
        task_id = self.__gen_task_id.__next__()
        return task_id

    async def start(self, queue: asyncio.Queue = None):
        self.__is_run = True
        try:
            self._queue = queue
            while True:
                data = await self._queue.get()
                self._queue.task_done()
                await self.put(data)
        finally:
            self.__is_run = False

    async def put(self):
        task_id = self.get_task_id()
        await self._master.create_task(
            "my_task_name", kwargs=dict(task_id=task_id)
        )

    @property
    def is_run(self):
        return self.__is_run

    async def close(self):
        await self._connection.close()


class Worker:
    _connection = None

    def __init__(self, routing_key, name):
        self.routing_key = routing_key
        self.name = name
        self.__is_run = False

    @classmethod
    async def create(cls, routing_key, uri, name):
        self = cls(routing_key, name)
        connection = await connect_robust(uri)
        self._connection = connection
        return self

    async def start(self):
        self.__is_run = False
        try:
            # Creating channel
            channel = await self._connection.channel()
            master = Master(channel)
            await master.create_worker("my_task_name", self.worker, auto_delete=False)
        finally:
            self.__is_run = False

    async def worker(self, *, task_id):
        # If you want to reject message or send
        # nack you might raise special exception

        # if task_id % 2 == 0:
        #     raise RejectMessage(requeue=False)
        #
        # if task_id % 2 == 1:
        #     raise NackMessage(requeue=False)

        print(self.name, task_id)

    @property
    def is_run(self):
        return self.__is_run


async def start_processed(app):
    await asyncio.sleep(1)
    app['rabbit_connections'] = []
    publisher = await Publisher.create('1_1', uri=app['config']['connection_rmq_uri'], name='publisher_1')
    app['rabbit_connections'].append(publisher)
    asyncio.create_task(publisher.start()).add_done_callback(functools.partial(done_back, 'publisher_1'))

    await asyncio.sleep(2)

    for n in range(1, 30):
        worker = await Worker.create('1_1', uri=app['config']['connection_rmq_uri'], name=f'worker_{n}')
        app['rabbit_connections'].append(worker)
        asyncio.create_task(worker.start()).add_done_callback(functools.partial(done_back, f'worker_{n}'))
        await asyncio.sleep(0)


def done_back(*args, **kwargs):
    name, _ = args
    logger.info(f' stop run {name}')

