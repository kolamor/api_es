import asyncio
import json
import logging

logger = logging.getLogger(__name__)


class Scheme:
    json_scheme_list = {'ElasticV1', }

    def __init__(self):
        pass

    @classmethod
    async def create(cls, json_scheme_paths: dict):
        self = cls()
        await self.load_scheme(json_scheme_paths)
        return self

    async def load_scheme(self,  json_scheme_paths: dict):
        tasks = []
        for schema in self.json_scheme_list:
            if schema in json_scheme_paths:
                task = asyncio.create_task(
                    self.set_load_json_validation_scheme(schema=schema, path=json_scheme_paths[schema]))
                tasks.append(task)
        await asyncio.gather(*tasks)

    async def set_load_json_validation_scheme(self, schema, path):
        loaded_json = await self.load_json_validation_scheme(path=path)
        setattr(self, schema, loaded_json)

    @classmethod
    async def load_json_validation_scheme(cls, path):
        """
        load json sheme from file, run_in_executor
        :return: dict
        """

        loop = asyncio.get_event_loop()
        s = await loop.run_in_executor(None, cls._load_json_file, path)
        logging.debug(f"load json_file {s}")
        return s

    @classmethod
    def _load_json_file(cls, path):
        with open(path, 'r') as f:
            config = json.loads(f.read())
        return config


async def loader_scheme(app, config=None):
    if config is None:
        config = app['config']
    scheme = await Scheme.create(config['json_scheme_path'])
    for key in config['json_scheme_path']:
        if not app.get('json_scheme', None):
            app['json_scheme'] = {}
        app['json_scheme'].update({key: getattr(scheme, key)})