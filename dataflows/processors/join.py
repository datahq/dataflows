import copy
import os
import collections

from kvfile import KVFile

from dataflows import PackageWrapper


# DB Helper
class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


# Aggregator helpers
def identity(x):
    return x


def median(values):
    if values is None:
        return None
    ll = len(values)
    mid = int(ll/2)
    values = sorted(values)
    if ll % 2 == 0:
        return (values[mid - 1] + values[mid])/2
    else:
        return values[mid]


def update_counter(curr, new):
    if new is None:
        return curr
    if curr is None:
        curr = collections.Counter()
    if isinstance(new, str):
        new = [new]
    if not isinstance(curr, collections.Counter):
        curr = collections.Counter(curr)
    curr.update(new)
    return curr


# Aggregators
Aggregator = collections.namedtuple('Aggregator',
                                    ['func', 'finaliser', 'dataType', 'copyProperties'])
AGGREGATORS = {
    'sum': Aggregator(lambda curr, new:
                      new + curr if curr is not None else new,
                      identity,
                      None,
                      False),
    'avg': Aggregator(lambda curr, new:
                      (curr[0] + 1, new + curr[1])
                      if curr is not None
                      else (1, new),
                      lambda value: value[1] / value[0],
                      None,
                      False),
    'median': Aggregator(lambda curr, new:
                         curr + [new] if curr is not None else [new],
                         median,
                         None,
                         True),
    'max': Aggregator(lambda curr, new:
                      max(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'min': Aggregator(lambda curr, new:
                      min(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'first': Aggregator(lambda curr, new:
                        curr if curr is not None else new,
                        identity,
                        None,
                        True),
    'last': Aggregator(lambda curr, new: new,
                       identity,
                       None,
                       True),
    'count': Aggregator(lambda curr, new:
                        curr+1 if curr is not None else 1,
                        identity,
                        'integer',
                        False),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None,
                      True),
    'set': Aggregator(lambda curr, new:
                      curr.union({new}) if curr is not None else {new},
                      lambda value: list(value) if value is not None else [],
                      'array',
                      False),
    'array': Aggregator(lambda curr, new:
                        curr + [new] if curr is not None else [new],
                        lambda value: value if value is not None else [],
                        'array',
                        False),
    'counters': Aggregator(lambda curr, new:
                           update_counter(curr, new),
                           lambda value:
                           list(collections.Counter(value).most_common()) if value is not None else [],
                           'array',
                           False),
}


# Input helpers
def fix_fields(fields):
    for field in sorted(fields.keys()):
        spec = fields[field]
        if spec is None:
            fields[field] = spec = {}
        if 'name' not in spec:
            spec['name'] = field
        if 'aggregate' not in spec:
            spec['aggregate'] = 'any'
    return fields


def concatenator(resources, all_target_fields, field_mapping):
    for resource_ in resources:
        for row in resource_:
            processed = dict((k, '') for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                    in row.items()
                    if k in field_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            yield processed


def join_aux(source_name, source_key, source_delete,  # noqa: C901
             target_name, target_key, fields, full):

    deduplication = target_key is None
    fields = fix_fields(fields)
    source_key = KeyCalc(source_key)
    target_key = KeyCalc(target_key) if target_key is not None else target_key
    db = KVFile()

    # Indexes the source data
    def indexer(resource):
        for row in resource:
            key = source_key(row)
            try:
                current = db.get(key)
            except KeyError:
                current = {}
            for field, spec in fields.items():
                name = spec['name']
                curr = current.get(field)
                agg = spec['aggregate']
                if agg != 'count':
                    new = row.get(name)
                else:
                    new = ''
                if new is not None:
                    current[field] = AGGREGATORS[agg].func(curr, new)
                elif field not in current:
                    current[field] = None
            db.set(key, current)
            yield row

    # Generates the joined data
    def process_target(resource):
        if deduplication:
            # just empty the iterable
            collections.deque(indexer(resource), maxlen=0)
            for key, value in db.items():
                row = dict(
                    (f, None) for f in fields.keys()
                )
                row.update(dict(
                    (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                    for k, v in value.items()
                ))
                yield row
        else:
            for row in resource:
                key = target_key(row)
                try:
                    extra = db.get(key)
                    extra = dict(
                        (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                        for k, v in extra.items()
                    )
                except KeyError:
                    if not full:
                        continue
                    extra = dict(
                        (k, row.get(k))
                        for k in fields.keys()
                    )
                row.update(extra)
                yield row

    # Yields the new resources
    def new_resource_iterator(resource_iterator):
        has_index = False
        for resource in resource_iterator:
            name = resource.res.name
            if name == source_name:
                has_index = True
                if source_delete:
                    # just empty the iterable
                    collections.deque(indexer(resource), maxlen=0)
                else:
                    yield indexer(resource)
                if deduplication:
                    yield process_target(resource)
            elif name == target_name:
                assert has_index
                yield process_target(resource)
            else:
                yield resource

    # Updates / creates the target resource descriptor
    def process_target_resource(source_spec, resource):
        target_fields = \
            resource.setdefault('schema', {}).setdefault('fields', [])
        added_fields = sorted(fields.keys())
        for field in added_fields:
            spec = fields[field]
            agg = spec['aggregate']
            data_type = AGGREGATORS[agg].dataType
            copy_properties = AGGREGATORS[agg].copyProperties
            to_copy = {}
            if data_type is None:
                try:
                    source_field = \
                        next(filter(lambda f, spec_=spec:
                                    f['name'] == spec_['name'],
                                    source_spec['schema']['fields']))
                except StopIteration:
                    raise KeyError('Failed to find field with name %s in resource %s' %
                                   (spec['name'], source_spec['name']))
                if copy_properties:
                    to_copy = copy.deepcopy(source_field)
                data_type = source_field['type']
            try:
                existing_field = next(iter(filter(
                    lambda f: f['name'] == field,
                    target_fields)))
                assert existing_field['type'] == data_type, \
                    'Reusing %s but with different data types: %s != %s' % (field, existing_field['type'], data_type)
            except StopIteration:
                to_copy.update({
                    'name': field,
                    'type': data_type
                })
                target_fields.append(to_copy)
        return resource

    # Updates the datapackage descriptor based on parameters
    def process_datapackage(datapackage):

        new_resources = []
        source_spec = None

        for resource in datapackage['resources']:

            if resource['name'] == source_name:
                source_spec = resource
                if not source_delete:
                    new_resources.append(resource)
                if deduplication:
                    resource = process_target_resource(
                        source_spec,
                        {
                            'name': target_name,
                            'path': os.path.join('data', target_name + '.csv')
                        })
                    new_resources.append(resource)

            elif resource['name'] == target_name:
                assert isinstance(source_spec, dict), \
                    "Source resource must appear before target resource"
                resource = process_target_resource(source_spec, resource)
                new_resources.append(resource)

            else:
                new_resources.append(resource)

        datapackage['resources'] = new_resources

    def func(package: PackageWrapper):
        process_datapackage(package.pkg.descriptor)
        yield package.pkg
        yield from new_resource_iterator(package)

    return func


def join(source_name, source_key, target_name, target_key, fields={}, full=True, source_delete=True):
    return join_aux(source_name, source_key, source_delete, target_name, target_key, fields, full)


def join_self(source_name, source_key, target_name, fields):
    return join_aux(source_name, source_key, True, target_name, None, fields, True)
