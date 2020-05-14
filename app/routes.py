from aiohttp import web
from .handlers import frontend
from .handlers import api


def setup_routes(app):
	app.router.add_route('GET', '/', frontend.index)
	app.router.add_route('GET', '/post', frontend.post)
	app.router.add_routes([
		web.post(r'/api/store{r:/?}', api.ParserV1, name='store_v1'),
	])

