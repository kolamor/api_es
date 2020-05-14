from aiohttp import web
import logging

logger = logging.getLogger(__name__)


class ParserV1(web.View):

    async def post(self):
        context = {
            "status": "ok"
        }
        return web.json_response(context, status=200, )
