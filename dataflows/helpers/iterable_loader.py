import itertools
import decimal
import datetime

from datapackage import Package, Resource
from tableschema.storage import Storage

from .. import DataStreamProcessor


class iterable_storage(Storage):

    def __init__(self, iterable):
        super(iterable_storage, self).__init__()
        self.iterable = iterable
        self.schema = None

    def connect(self, name): pass
    def buckets(self): pass
    def create(self): pass
    def delete(self): pass
    def read(self): pass
    def write(self): pass

    def field_type(self, value):
        if isinstance(value, str):
            return 'string'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, (float, decimal.Decimal)):
            return 'number'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        elif isinstance(value, datetime.datetime):
            return 'datetime'
        elif isinstance(value, datetime.date):
            return 'date'
        elif value is None:
            return 'any'
        assert 'Unknown Python type: %r' % value

    def describe(self, _, descriptor=None):
        if descriptor is not None:
            return descriptor
        if self.schema is None:
            try:
                rec = next(self.iterable)
                self.iterable = itertools.chain([rec], self.iterable)
                self.schema = dict(
                    fields=[
                        dict(name=name, type=self.field_type(value))
                        for name, value in rec.items()
                    ]
                )
            except Exception:
                self.schema = dict(fields=[])
        return self.schema

    def iter(self, _):
        return self.iterable


class iterable_loader(DataStreamProcessor):

    def __init__(self, iterable, name=None):
        super(iterable_loader, self).__init__()
        self.iterable = iterable
        self.name = name
        self.exc = None

    def handle_iterable(self):
        mode = None
        try:
            for x in self.iterable:
                if mode is None:
                    assert isinstance(x, (dict, list))
                    mode = dict if isinstance(x, dict) else list
                assert isinstance(x, mode)
                if mode == dict:
                    yield x
                else:
                    yield dict(zip(('col{}'.format(i) for i in range(len(x))), x))
        except Exception as e:
            self.exc = e
            raise

    def process_datapackage(self, dp: Package):
        name = self.name
        if name is None:
            name = 'res_{}'.format(len(dp.resources) + 1)
        self.res = Resource(dict(
            name=name,
            path='{}.csv'.format(name)
        ), storage=iterable_storage(self.handle_iterable()))
        self.res.infer()
        if self.exc is not None:
            raise self.exc
        dp.descriptor.setdefault('resources', []).append(self.res.descriptor)
        return dp

    def process_resources(self, resources):
        yield from super(iterable_loader, self).process_resources(resources)
        yield self.res.iter(keyed=True)
