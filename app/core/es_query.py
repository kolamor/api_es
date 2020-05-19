import asyncio
from aioelasticsearch import Elasticsearch, AIOHttpConnectionPool, AIOHttpTransport
import logging

logger = logging.getLogger(__name__)


class NoSetDocumentId(Exception):
    pass


class FbcInstanceElasticsearch:
    """
    Factory Singleton instance Elasticsearch
    """
    instance = None

    @classmethod
    def _create(cls, *args, **kwargs) -> Elasticsearch:
        inst = Elasticsearch(*args, **kwargs)
        cls.instance = inst
        return inst

    @classmethod
    def get(cls, *args, **kwargs):
        if isinstance(cls.instance, Elasticsearch):
            return cls.instance
        else:
            return cls._create(*args, **kwargs)


class BaseElasticsearchQuery:

    def __init__(self, *args, **kwargs):
        self._es = FbcInstanceElasticsearch.get(*args, **kwargs)

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
        self.index = kwargs.get('index')
        super().__init__(*args, **kwargs)

    async def save(self, doc, doc_id=None):
        _doc_id = doc.pop('_id', None)
        if doc_id is None:
            doc_id = _doc_id
        if doc_id is None:
            raise NoSetDocumentId('No set document id')
        res = await self._es.index(index=self.index, id=doc_id, body=doc)
        return res

    async def exist_id(self, doc_id):
        res = await self._es.exists(index=self.index, id=doc_id)
        return res







