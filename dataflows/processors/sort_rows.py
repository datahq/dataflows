import re
import datetime
from kvfile import KVFile
from ..helpers.resource_matcher import ResourceMatcher


class KeyCalc(object):
    def __init__(self, key_spec):
        self.key_spec = key_spec
        self.key_list = re.findall(r'\{(.*?)\}', key_spec)

    def __call__(self, row):
        context = row.copy()
        for key, value in row.items():
            # We need to stringify some values to make them properly comparable
            if key in self.key_list:
                # numbers
                # 1000 -> +1.000000e+03 -> p03ep1.000000
                if isinstance(value, (int, float)):
                    value = 'e'.join(reversed('{:+e}'.format(value).split('e')))
                    value = value.replace('+', 'p').replace('-', 'm')
                # date/time
                # **** -> iso format
                elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                    value = value.isoformat()
                context[key] = value
        return self.key_spec.format(**context)


def _sorter(rows, key_calc, reverse, batch_size):
    db = KVFile()
    def process(rows):
        for row_num, row in enumerate(rows):
            key = key_calc(row) + "{:08x}".format(row_num)
            yield (key, row)
    db.insert(process(rows), batch_size=batch_size)
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
