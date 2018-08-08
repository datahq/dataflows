import os
from datapackage import Package
from dataflows import PackageWrapper, DataStream, Flow
from . import dump_to_path


def cache(cache_path, *steps):
    cache_package_json_path = os.path.join(cache_path, 'datapackage.json')

    def processor(package: PackageWrapper):
        if os.path.exists(cache_package_json_path):
            print('using cache data from {}'.format(cache_path))
            dp = Package(cache_package_json_path)
            res_iter = (resource.iter(keyed=True) for resource in dp.resources)
        else:
            print('loading fresh data, saving cache to: {}'.format(cache_path))
            ds: DataStream = Flow(*steps + (dump_to_path(cache_path),))._chain()._process()
            dp = ds.dp
            res_iter = ds.res_iter
        for resource in dp.resources:
            package.pkg.add_resource(resource.descriptor)
        yield package.pkg
        yield from package
        yield from res_iter

    return processor
