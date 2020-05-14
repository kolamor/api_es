import aiohttp_jinja2
import aio_pika
import asyncpgsa
import jinja2
from aiohttp import web
from .routes import setup_routes


async def create_app(config: dict):
    app = web.Application()
    app['config'] = config
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.PackageLoader('app', 'templates'),
    )
    setup_routes(app)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_shutdown)

    return app


async def on_start(app):
    config = app['config']
    app['db'] = await asyncpgsa.create_pool(dsn=config['database_uri'])
    app['connection_rmq'] = await aio_pika.connect_robust(config['connection_rmq'])


async def on_shutdown(app):
    await app['db'].close()
    await app['connection_rmq'].close()
