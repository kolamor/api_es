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
                tasks.append(asyncio.create_task(self.save_storage(json_data)))
        except ValidationError as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}, "validation": str(e.args)},
                                     status=400, )
        except Exception as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}}, status=400, )
        if isinstance(json_data, list):
            for row in json_data:
                try:
                    validate(instance=row, schema=self.request.app["json_scheme"]["ElasticV1"])
                except ValidationError as e:
                    logger.error(f' {self.__class__.__name__} post {e.args}')
                    return web.json_response({"status 400": "Bad Request", "template": {}, "doc": row}, status=400, )
                task = asyncio.create_task(self.save_storage(row))
                tasks.append(task)
        res = await asyncio.gather(*tasks, return_exceptions=True)
        if self.request.app['config'].get('logging_level', 'INFO') != 'DEBUG':
            res = 'in process'
        context = {
            "status": str(res)
        }
        return web.json_response(context, status=200, )

    async def save_storage(self, row):
        res = await self.request.app['esearch'].save(row)
        return res


class ElasticV2(ElasticV1):
    async def save_storage(self, row):
        queue = self.request.app.get('queue_v2')
        if queue:
            await queue.put(row)
        else:
            return 'Don\'t work'
        return 'Ok'
