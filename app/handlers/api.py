from aiohttp import web
import asyncio
from jsonschema import validate, ValidationError

import logging

logger = logging.getLogger(__name__)


class ElasticV1(web.View):
    """
    Saving directly to the database Elasticsearch
    """

    async def post(self):
        tasks = []
        try:
            json_data = await self.request.json()
            if isinstance(json_data, dict):
                validate(instance=json_data, schema=self.request.app["json_scheme"]["ElasticV1"])
                tasks.append(asyncio.create_task(self.request.app['esearch'].save(json_data)))
        except ValidationError as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}}, status=400, )
        except Exception as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}}, status=400, )
        if isinstance(json_data, list):
            for row in json_data:
                validate(instance=row, schema=self.request.app["json_scheme"]["ElasticV1"])
                task = asyncio.create_task(self.request.app['esearch'].save(row))
                tasks.append(task)
        res = await asyncio.gather(*tasks, return_exceptions=True)
        if self.request.app['config'].get('logging_level', 'INFO') != 'DEBUG':
            res = 'in process'
        context = {
            "status": str(res)
        }
        return web.json_response(context, status=200, )
