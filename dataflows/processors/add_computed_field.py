import functools
import collections

from ..helpers.resource_matcher import ResourceMatcher

Aggregator = collections.namedtuple('Aggregator', ['func'])

AGGREGATORS = {
    'sum': Aggregator(lambda values, fstr, row: sum(values)),
    'avg': Aggregator(lambda values, fstr, row: sum(values) / len(values)),
    'max': Aggregator(lambda values, fstr, row: max(values)),
    'min': Aggregator(lambda values, fstr, row: min(values)),
    'multiply': Aggregator(
        lambda values, fstr, row: functools.reduce(lambda x, y: x*y, values)),
    'constant': Aggregator(lambda values, fstr, row: fstr),
    'join': Aggregator(
        lambda values, fstr, row: fstr.join([str(x) for x in values])),
    'format': Aggregator(lambda values, fstr, row: fstr.format(**row)),
}


def get_type(res_fields, operation_fields, operation):
    types = [f.get('type') for f in res_fields if f['name'] in operation_fields]
    if 'any' in types:
        return 'any'
    if (operation == 'format') or (operation == 'join'):
        return 'string'
    if ('number' in types) or (operation == 'avg'):
        return 'number'
    # integers
    if len(types):
        return types[0]
    # constant
    return 'any'


def process_resource(fields, rows):
    for row in rows:
        for field in fields:
            values = [
                row.get(c) for c in field.get('source', []) if row.get(c) is not None
            ]
            with_ = field.get('with', field.get('with_', ''))
            new_col = AGGREGATORS[field['operation']].func(values, with_, row)
            row[field['target']] = new_col
        yield row


def add_computed_field(fields, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                descriptor = resource
                new_fields = [
                    {
                        'name': f['target'],
                        'type': get_type(descriptor['schema']['fields'],
                                        f.get('source', []),
                                        f['operation'])
                    } for f in fields
                ]
                descriptor['schema']['fields'].extend(new_fields)
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(fields, resource)

    return func
