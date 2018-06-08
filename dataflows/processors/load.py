from datapackage import Package, Resource
from .. import DataStreamProcessor


class load(DataStreamProcessor):

    def __init__(self, path, name=None, **options):
        super(load, self).__init__()
        self.path = path
        self.options = options
        self.name = name

    def process_datapackage(self, dp: Package):
        self.res = Resource(dict(path=self.path, **self.options))
        self.res.infer()
        if self.name is not None:
            self.res.descriptor['name'] = self.name
        dp.add_resource(self.res.descriptor)
        return dp

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        yield self.res.iter(keyed=True)
