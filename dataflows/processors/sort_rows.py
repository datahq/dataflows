from kvfile import KVFile

from ..helpers.resource_matcher import ResourceMatcher


class KeyCalc(object):
    def __init__(self, key_spec):
        self.key_spec = key_spec

    def __call__(self, row):
        return self.key_spec.format(**row)


def _sorter(rows, key_calc, reverse, batch_size):
    db = KVFile()
    db.insert(((key_calc(row) + "{:08x}".format(row_num), row) for row_num, row in enumerate(rows)),
              batch_size=batch_size)

    for _, value in db.items(reverse=reverse):
        yield value


def sort_rows(key, resources=None, reverse=False, batch_size=1000):
    key_calc = KeyCalc(key)

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield _sorter(rows, key_calc, reverse, batch_size)
            else:
                yield rows

    return func
