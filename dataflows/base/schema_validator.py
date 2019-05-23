from datapackage import Resource
from tableschema import Schema
from tableschema.exceptions import CastError


class ValidationError(Exception):

    def __init__(self, resource_name, row, index, cast_error):
        msg = '\nROW: %r\n----\n' % row
        if cast_error is not None and hasattr(cast_error, 'errors'):
            msg += '\n'.join('%d) %s' % (i+1, err)
                            for i, err
                            in enumerate(cast_error.errors))
        super().__init__(msg)
        self.resource_name = resource_name
        self.row = row
        self.index = index
        self.cast_error = cast_error


def raise_exception(res_name, row, i, e):
    raise ValidationError(res_name, row, i, e)


def ignore(res_name, row, i, e):
    return True


def drop(res_name, row, i, e):
    return False


def schema_validator(resource, iterator,
                     field_names=None, on_error=None):
    if on_error is None:
        on_error = raise_exception
    if isinstance(resource, Resource):
        schema: Schema = resource.schema
        assert schema is not None
        resource = resource.descriptor
    else:
        schema: Schema = Schema(resource.get('schema', {}))
    if field_names is None:
        field_names = [f.name for f in schema.fields]
    schema_fields = [f for f in schema.fields if f.name in field_names]
    for i, row in enumerate(iterator):
        try:
            for f in schema_fields:
                row[f.name] = f.cast_value(row.get(f.name))
        except CastError as e:
            if not on_error(resource['name'], row, i, e):
                continue

        yield row
