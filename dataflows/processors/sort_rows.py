from kvfile import KVFile

from ..helpers.resource_matcher import ResourceMatcher


class KeyCalc(object):
    def __init__(self, key_spec):
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


def _sorter(rows, key_calc):
    db = KVFile()
    for row_num, row in enumerate(rows):
        key = key_calc(row) + "{:08x}".format(row_num)
        db.set(key, row)
    for _, value in db.items():
        yield value


def sort_rows(key, resources=None):
    matcher = ResourceMatcher(resources)
    key_calc = KeyCalc(key)

    def func(rows):
        if matcher.match(rows.res.name):
            yield from _sorter(rows, key_calc)
        else:
            yield from rows

    return func

