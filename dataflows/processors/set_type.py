from itertools import islice
from datapackage import Resource
from .. import DataStreamProcessor, schema_validator


class set_type(DataStreamProcessor):

    def __init__(self, name, **options):
        super(set_type, self).__init__()
        self.name = name
        self.options = options
        self.resource = None
        self.resource_count = None

    def process_resources(self, resources):
        resources = super(set_type, self).process_resources(resources)
        yield from islice(resources, self.resource_count - 1)
        yield schema_validator(self.resource, next(resources))

    def process_datapackage(self, dp):
        dp = super(set_type, self).process_datapackage(dp)
        added = False
        for field in dp.descriptor['resources'][-1]['schema']['fields']:
            if field['name'] == self.name:
                field.update(self.options)
                added = True
                break
        self.resource = Resource(dp.descriptor['resources'][-1])
        self.resource_count = len(dp.descriptor['resources'])
        assert added, 'Failed to find field {} in schema'.format(self.name)
        return dp
