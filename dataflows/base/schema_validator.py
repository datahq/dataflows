from datapackage import Resource
from tableschema import Schema
from tableschema.exceptions import CastError


class ValidationError(Exception):

    def __init__(self, resource_name, row, index, cast_error):
        msg = '\nROW: %r\n----\n' % row
        msg += '\n'.join('%d) %s' % (i+1, err)
                         for i, err
                         in enumerate(cast_error.errors))
        super().__init__(msg)
        self.resource_name = resource_name
        self.row = row
        self.index = index
        self.cast_error = cast_error


def schema_validator(resource: Resource, iterator, field_names=None):
    schema: Schema = resource.schema
    if field_names is None:
        field_names = [f.name for f in schema.fields]
    schema_fields = [f for f in schema.fields if f.name in field_names]
    for i, row in enumerate(iterator):

        try:
            for f in schema_fields:
                row[f.name] = f.cast_value(row.get(f.name))
        except CastError as e:
            raise ValidationError(resource.name, row, i, e)

        yield row
