import os
from dataflows import Flow, DataStream
from . import dump_to_path, load


def cache(*steps, cache_path):
    cache_package_json_path = os.path.join(cache_path, 'datapackage.json')

    def processor(package):
        if os.path.exists(cache_package_json_path):
            print('using cache data from {}'.format(cache_path))
            flow = Flow(load(cache_package_json_path, resources=None))
        else:
            print('loading fresh data, saving cache to: {}'.format(cache_path))
            flow = Flow(*steps + (dump_to_path(cache_path),))
        ds = flow.datastream(DataStream(package.pkg, package))
        yield ds.dp
        yield from ds.res_iter

    return processor
