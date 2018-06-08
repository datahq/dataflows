from inspect import isfunction, signature
from collections import Iterable

from .datastream_processor import DataStreamProcessor


class Flow:
    def __init__(self, *args):
        self.chain = args

    def results(self):
        return self._chain().results()

    def process(self):
        return self._chain().process()

    def _chain(self):
        from ..helpers import datapackage_processor, rows_processor, row_processor, iterable_loader

        ds = None
        for link in self.chain:
            if isinstance(link, DataStreamProcessor):
                ds = link(ds)
            elif isfunction(link):
                sig = signature(link)
                params = list(sig.parameters)
                if len(params) == 1:
                    if params[0] == 'row':
                        ds = row_processor(link)(ds)
                    elif params[0] == 'rows':
                        ds = rows_processor(link)(ds)
                    elif params[0] == 'package':
                        ds = datapackage_processor(link)(ds)
                    else:
                        assert False, 'Failed to parse function signature %r' % params
                else:
                    assert False, 'Failed to parse function signature %r' % params
            elif isinstance(link, Iterable):
                ds = iterable_loader(link)(ds)

        return ds
