import os

from datapackage import Package, Resource
from .. import DataStreamProcessor
from ..base.schema_validator import schema_validator
from ..helpers.resource_matcher import ResourceMatcher


class load(DataStreamProcessor):

    def __init__(self, load_source, name=None, resources=None, validate=False, **options):
        super(load, self).__init__()
        self.load_source = load_source
        self.options = options
        self.name = name
        self.resources = resources
        self.load_dp = None
        self.validate = validate

    def process_datapackage(self, dp: Package):
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, _ = self.load_source
            dp.descriptor.setdefault('resources', [])
            self.resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor['resources']:
                if self.resource_matcher.match(resource_descriptor['name']):
                    dp.add_resource(resource_descriptor)
        else:  # load_source is string:
            if self.load_source.startswith('env://'):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")
            if os.path.basename(self.load_source) == 'datapackage.json':
                self.load_dp = Package(self.load_source)
                self.resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                dp.descriptor.setdefault('resources', [])
                for resource in self.load_dp.resources:
                    if self.resource_matcher.match(resource.name):
                        dp.add_resource(resource.descriptor)
            else:
                if os.path.exists(self.load_source):
                    base_path = os.path.dirname(self.load_source) or '.'
                    self.load_source = os.path.basename(self.load_source)
                else:
                    base_path = None
                descriptor = dict(path=self.load_source,
                                  profile='tabular-data-resource')
                descriptor['format'] = self.options.get('format')
                self.options.setdefault('ignore_blank_headers', True)
                self.options.setdefault('headers', 1)
                self.res = Resource(descriptor,
                                    base_path=base_path,
                                    **self.options)
                self.res.infer(confidence=1, limit=1000)
                if self.name is not None:
                    self.res.descriptor['name'] = self.name
                self.res.descriptor['path'] = '{name}.{format}'.format(**self.res.descriptor)
                dp.add_resource(self.res.descriptor)
        return dp

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resources = self.load_source
            yield from (resource for resource, descriptor in zip(resources, datapackage_descriptor['resources'])
                        if self.resource_matcher.match(descriptor['name']))
        elif self.load_dp is not None:
            yield from (resource.iter(keyed=True) for resource in self.load_dp.resources
                        if self.resource_matcher.match(resource.name))
        else:
            it = self.res.iter(keyed=True, cast=False)
            if self.validate:
                it = schema_validator(self.res, it)
            yield it
