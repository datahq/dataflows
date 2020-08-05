from tableschema import Schema
from tableschema.exceptions import CastError
from ..helpers.resource_matcher import ResourceMatcher
from .. import DataStreamProcessor


class collect_errors(DataStreamProcessor):

    def __init__(self, schema, resources=-1):
        super().__init__()
        self.schema = Schema(schema)
        self.resources = resources

    def process_resources(self, resources):
        for res in resources:
            if self.matcher.match(res.res.name):
                yield self.collect_errors(res)
            else:
                yield res

    def collect_errors(self, resource):
        errors = []
        for row in resource:
            for field in self.schema.fields:
                try:
                    field.cast_value(row.get(field.name))
                except CastError as error:
                    errors.append(str(error))
            yield row
        for descriptor in self.dp.descriptor['resources']:
            if descriptor.get('name') == resource.res.name:
                descriptor['errors'] = errors
        self.dp.commit()

    def process_datapackage(self, dp):
        dp = super().process_datapackage(dp)
        self.matcher = ResourceMatcher(self.resources, dp)
        self.dp = dp
        return dp
