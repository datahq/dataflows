import os

from datapackage import Package, Resource
from .. import DataStreamProcessor


class load(DataStreamProcessor):

    def __init__(self, path, name=None, **options):
        super(load, self).__init__()
        self.path = path
        self.options = options
        self.name = name

    def process_datapackage(self, dp: Package):
        if os.path.exists(self.path):
            base_path = os.path.dirname(self.path)
            self.path = os.path.basename(self.path)
        else:
            base_path = None
        self.res = Resource(dict(path=self.path, **self.options), base_path=base_path)
        self.res.infer(infer_options={'confidence': 1, 'limit': 1000})
        if self.name is not None:
            self.res.descriptor['name'] = self.name
        self.res.descriptor['path'] = '{name}.{format}'.format(**self.res.descriptor)
        dp.add_resource(self.res.descriptor)
        return dp

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        yield self.res.iter(keyed=True)
