import asyncio
from uuid import uuid4
import logging
import functools
from aio_pika import connect_robust, Message
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
                _task_id = f'{self._uuid}_{n}'
                yield _task_id
                n += 1

        if not isgenerator(self.__gen_task_id):
            self.__gen_task_id = gen()
        task_id = self.__gen_task_id.__next__()
        return task_id

    async def start(self, queue: asyncio.Queue):
        self.__is_run = True
        try:
            self._queue = queue
            while True:
                data = await self._queue.get()
                self._queue.task_done()
                await self.put(data)
        finally:
            self.__is_run = False

    async def put(self, data):
        task_id = self.get_task_id()
        await self._master.create_task(
            self.routing_key, kwargs=dict(task_id=task_id, body=data),
        )

    @property
    def is_run(self):
        return self.__is_run

    async def close(self):
        await self._connection.close()


class Worker:
    _connection = None

    def __init__(self, routing_key, name, processor):
        self.routing_key = routing_key
        self.name = name
        self.__is_run = False
        self._processor = processor

    @classmethod
    async def create(cls, routing_key, uri, name, processor):
        self = cls(routing_key, name, processor)
        connection = await connect_robust(uri)
        self._connection = connection
        return self

    async def start(self):
        self.__is_run = False
        try:
            # Creating channel
            channel = await self._connection.channel()
            master = Master(channel)
            await master.create_worker(self.routing_key, self.worker, auto_delete=False)
        finally:
            self.__is_run = False

    async def worker(self, *, body, task_id):
        print(body)
        # If you want to reject message or send
        # nack you might raise special exception

        # if task_id % 2 == 0:
        #     raise RejectMessage(requeue=False)
        #
        # if task_id % 2 == 1:
        #     raise NackMessage(requeue=False)

        print(self.name, task_id)
        res = await self._processor.start(body)
        print(res)

    def set_processor(self, processor):
        self._processor = processor

    @property
    def is_run(self):
        return self.__is_run

    async def close(self):
        await self._connection.close()


class Processor:

    def __init__(self, es):
        self.es = es

    async def start(self, data):
        res = await self.es.save(data)
        return res


async def start_processed(app):
    await asyncio.sleep(1)
    app['rabbit_connections'] = []
    publisher = await Publisher.create('1_1', uri=app['config']['connection_rmq_uri'], name='publisher_1',)
    app['rabbit_connections'].append(publisher)
    asyncio.create_task(publisher.start(queue=app['queue_v2'])).add_done_callback(functools.partial(done_back,
                                                                                              publisher.name))
    await asyncio.sleep(1)
    processor = Processor(app['esearch'])
    for n in range(1, 2):
        worker = await Worker.create('1_1', uri=app['config']['connection_rmq_uri'], name=f'worker_{n}',
                                     processor=processor)
        app['rabbit_connections'].append(worker)
        await worker.start()


def done_back(*args, **kwargs):
    name, _ = args
    logger.info(f' stop run {name}')

