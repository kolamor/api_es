import asyncio
from aioelasticsearch import Elasticsearch, AIOHttpConnectionPool, AIOHttpTransport
import logging

logger = logging.getLogger(__name__)


class BaseElasticsearchQuery:

    def __init__(self, *args, **kwargs):
        self._es = Elasticsearch(*args, **kwargs)

    @classmethod
    async def create(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        return self

    async def close(self):
        await self._es.close()

    async def __aenter__(self):  # noqa
        return self

    async def __aexit__(self, *exc_info):  # noqa
        await self.close()


class EsQuery(BaseElasticsearchQuery):
    def __init__(self, *args,  **kwargs):
        super().__init__(*args, **kwargs)





