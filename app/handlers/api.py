from aiohttp import web
from jsonschema import validate, ValidationError

import logging

logger = logging.getLogger(__name__)


class ElasticV1(web.View):
    """
    Saving directly to the database Elasticsearch
    """

    async def post(self):
        try:
            json_data = await self.request.json()
            validate(instance=json_data, schema=self.request.app["json_scheme"]["ElasticV1"])
        except ValidationError as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}}, status=400, )
        except Exception as e:
            logger.error(f' {self.__class__.__name__} post {e.args}')
            return web.json_response({"status 400": "Bad Request", "template": {}}, status=400, )


        context = {
            "status": "ok"
        }
        return web.json_response(context, status=200, )
