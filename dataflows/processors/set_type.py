import re

from ..helpers.resource_matcher import ResourceMatcher
from .. import DataStreamProcessor, schema_validator


class set_type(DataStreamProcessor):

    def __init__(self, name, resources=-1, **options):
        super(set_type, self).__init__()
        self.name = re.compile(f'^{name}$')
        self.options = options
        self.resources = resources

    def process_resources(self, resources):
        for res in resources:
            if self.matcher.match(res.res.name):
                yield schema_validator(res.res, res)
            else:
                yield res

    def process_datapackage(self, dp):
        dp = super(set_type, self).process_datapackage(dp)
        self.matcher = ResourceMatcher(self.resources, dp)
        added = False
        for res in dp.descriptor['resources']:
            if self.matcher.match(res['name']):
                for field in res['schema']['fields']:
                    if self.name.match(field['name']):
                        field.update(self.options)
                        added = True
        assert added, 'Failed to find field {} in schema'.format(self.name)
        return dp
