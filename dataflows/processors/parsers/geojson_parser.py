from tabulator.helpers import reset_stream
from tabulator.parsers.json import JSONParser


class GeoJsonParser(JSONParser):
    options = []

    def __init__(self, loader, force_parse=False, property=None):
        super().__init__(loader, force_parse=force_parse, property='features')
        self.__extended_rows = None

    @property
    def extended_rows(self):
        iterator = super().extended_rows
        for row_number, keys, values in iterator:
            row = dict(zip(keys, values))
            properties = row.get('properties', dict())
            properties['__geometry'] = row.get('geometry')
            items = list(properties.items())
            yield row_number, list(x[0] for x in items), list(x[1] for x in items)

