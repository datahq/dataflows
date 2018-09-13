from .. import DataStreamProcessor, schema_validator


class validate(DataStreamProcessor):

    def __init__(self):
        super(validate, self).__init__()

    def process_resource(self, res):
        yield from super(validate, self).process_resource(schema_validator(res.res, res))
