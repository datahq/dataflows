import os

from datapackage import Package, Resource
from .. import DataStreamProcessor
from ..helpers.resource_matcher import ResourceMatcher


class load(DataStreamProcessor):

    def __init__(self, path, name=None, resources=False, **options):
        super(load, self).__init__()
        self.path = path
        self.options = options
        self.name = name
        if resources is False:
            self.resource_matcher = None
        else:
            self.resource_matcher = ResourceMatcher(resources)

    def process_datapackage(self, dp: Package):
        if self.resource_matcher:
            self.load_dp = Package(self.path)
            for resource in self.load_dp.resources:
                if self.resource_matcher.match(resource.name):
                    dp.add_resource(resource.descriptor)
        else:
            if os.path.exists(self.path):
                base_path = os.path.dirname(self.path)
                self.path = os.path.basename(self.path)
            else:
                base_path = None
            self.res = Resource(dict(path=self.path,
                                     profile='tabular-data-resource',
                                     **self.options), base_path=base_path)
            self.res.infer(confidence=1, limit=1000)
            if self.name is not None:
                self.res.descriptor['name'] = self.name
            self.res.descriptor['path'] = '{name}.{format}'.format(**self.res.descriptor)
            dp.add_resource(self.res.descriptor)
        return dp

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        if self.resource_matcher:
            yield from (resource.iter(keyed=True) for resource in self.load_dp.resources
                        if self.resource_matcher.match(resource.name))
        else:
            yield self.res.iter(keyed=True)
