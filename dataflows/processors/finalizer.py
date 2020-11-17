from inspect import signature

from .. import DataStreamProcessor

class finalizer(DataStreamProcessor):

    def __init__(self, callback):
        super().__init__()
        self.callback = callback

    def safe_process(self, on_error=None):
        ds, results = super().safe_process(on_error=on_error)
        if 'stats' in signature(self.callback).parameters:
            stats = ds.merge_stats()
            self.callback(stats=stats)
        else:
            self.callback()
        return ds, results