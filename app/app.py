import aiohttp_jinja2
import aio_pika
import asyncpgsa
import asyncio
import jinja2
import logging
from aiohttp import web
from .routes import setup_routes

logger = logging.getLogger(__name__)


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
    if config.get('start_connection_rmq', False):
        app['connection_rmq'] = await aio_pika.connect_robust(config['connection_rmq_uri'])


async def on_shutdown(app):
    logger.info('on_shutdown')
    await app['db'].close()
    logger.info('PSQL closed')
    if app.get('connection_rmq', None):
        logger.info('on_shutdown connection_rmq')
        await app['connection_rmq'].close()
        # aio_pika.connection:Closing AMQP connection None
        # aio_pika.robust_connection:Connection to amqp://user:******@127.0.0.1/ closed. Reconnecting after 5 seconds.
        # -- https://github.com/mosquito/aio-pika/issues/314
        logger.info('connection_rmq closed')
