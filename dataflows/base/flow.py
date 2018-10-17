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

    def datastream(self, ds=None):
        return self._chain(ds)._process()

    def _preprocess_chain(self):
        checkpoint_links = []
        for link in self.chain:
            if hasattr(link, 'handle_flow_checkpoint'):
                checkpoint_links = link.handle_flow_checkpoint(checkpoint_links)
            else:
                checkpoint_links.append(link)
        return checkpoint_links

    def _chain(self, ds=None):
        from ..helpers import datapackage_processor, rows_processor, row_processor, iterable_loader

        for link in self._preprocess_chain():
            if isinstance(link, Flow):
                ds = link._chain(ds)
            elif isinstance(link, DataStreamProcessor):
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
