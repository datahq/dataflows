import logging

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


def schema_validator(resource: Resource, iterator):
    schema: Schema = resource.schema
    field_names = [f.name for f in schema.fields]
    warned_fields = set()
    for i, row in enumerate(iterator):
        to_cast = [row.get(f) for f in field_names]
        try:
            casted = schema.cast_row(to_cast)
            row = dict(zip(field_names, casted))
        except CastError as e:
            raise ValidationError(resource.name, row, i, e)

        for k in set(row.keys()) - set(field_names):
            if k not in warned_fields:
                warned_fields.add(k)
                logging.warning('Encountered field %r, not in schema', k)

        yield row
