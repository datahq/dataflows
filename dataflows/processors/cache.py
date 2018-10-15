import os
from itertools import chain
from dataflows import Flow
from . import dump_to_path, load


class cache(Flow):

    def __init__(self, *steps, cache_path, resources=None):
        super().__init__(*steps)
        self.cache_path = cache_path
        self.resources = resources

    def _chain(self, *args, **kwargs):
        cache_package_json_path = os.path.join(self.cache_path, 'datapackage.json')
        if os.path.exists(cache_package_json_path):
            print('using cache data from {}'.format(self.cache_path))
            self.chain = load(cache_package_json_path, resources=self.resources),
        else:
            print('loading fresh data, saving cache to: {}'.format(self.cache_path))
            self.chain = chain(self.chain, (dump_to_path(self.cache_path, resources=self.resources),))
        return super()._chain(*args, **kwargs)


class CacheFlow(Flow):

    def __init__(self, *steps):
        cache_steps = []
        for step in steps:
            if isinstance(step, cache):
                step.chain = tuple(cache_steps)
                cache_steps = [step]
            else:
                cache_steps.append(step)
        super().__init__(*cache_steps)
