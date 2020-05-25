from .. import DataStreamProcessor


class conditional(DataStreamProcessor):

    def __init__(self, predicate, flow):
        super().__init__()
        self.predicate = predicate
        self.flow = flow

    def _process(self):
        ds = self.source._process()
        if self.predicate(ds.dp):
            return self.flow.datastream(ds)
        else:
            return ds
