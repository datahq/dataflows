
import logging

from datapackage import Resource
from tableschema import Schema
from tableschema.exceptions import CastError


def schema_validator(resource: Resource, iterator):
    schema: Schema = resource.schema
    field_names = [f.name for f in schema.fields]
    warned_fields = set()
    for row in iterator:
        to_cast = [row.get(f) for f in field_names]
        try:
            casted = schema.cast_row(to_cast)
            row = dict(zip(field_names, casted))
        except CastError as e:
            msg = '\nROW: %r\n----\n' % row
            msg += '\n'.join('%d) %s' % (i+1, err) for i, err in enumerate(e.errors))
            raise ValueError(msg) from None

        for k in set(row.keys()) - set(field_names):
            if k not in warned_fields:
                warned_fields.add(k)
                logging.warning('Encountered field %r, not in schema', k)

        yield row
