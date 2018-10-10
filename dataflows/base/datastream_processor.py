import logging
import itertools
import collections
import copy

from datapackage import Package
from tableschema.exceptions import CastError

from .datastream import DataStream
from .resource_wrapper import ResourceWrapper
from .schema_validator import schema_validator


class DataStreamProcessor:

    def __init__(self):
        self.stats = {}
        self.source = None
        self.datapackage = None

    def __call__(self, source=None):
        if source is None:
            source = DataStream()
        self.source = source
        return self

    def process_resource(self, resource: ResourceWrapper):
        for row in resource:
            yield self.process_row(row)

    def process_resources(self, resources):
        for res in resources:
            yield self.process_resource(res)

    def process_row(self, row):
        return row

    def process_datapackage(self, dp: Package):
        return dp

    def _process(self):
        datastream = self.source._process()
        current_dp = datastream.dp

        self.datapackage = Package(descriptor=copy.deepcopy(datastream.dp.descriptor))
        self.datapackage = self.process_datapackage(self.datapackage)
        self.datapackage.commit()

        res_iter = datastream.res_iter

        def get_res(name):
            ret = self.datapackage.get_resource(name)
            if ret is None:
                ret = current_dp.get_resource(name)
            assert ret is not None
            return ret

        res_iter = (ResourceWrapper(get_res(rw.res.name), rw.it)
                    for rw in res_iter)
        res_iter = self.process_resources(res_iter)
        res_iter = (it if isinstance(it, ResourceWrapper) else ResourceWrapper(res, it)
                    for res, it
                    in itertools.zip_longest(self.datapackage.resources, res_iter))

        return DataStream(self.datapackage, res_iter, datastream.stats + [self.stats])

    def process(self):
        ds = self._process()
        try:
            for res in ds.res_iter:
                collections.deque(res, maxlen=0)
        except CastError as e:
            for err in e.errors:
                logging.error('%s', err)
        return ds.dp, ds.merge_stats()

    def results(self):
        ds = self._process()
        results = [
            list(schema_validator(res.res, res))
            for res in ds.res_iter
        ]
        return results, ds.dp, ds.merge_stats()
