import re
import decimal
from kvfile import KVFile
from ..helpers.resource_matcher import ResourceMatcher


class KeyCalc(object):
    def __init__(self, key_spec):
        self.key_spec = key_spec
        self.key_list = re.findall(r'\{(.*?)\}', key_spec)

    def __call__(self, row):
        context = row.copy()
        for key, value in row.items():
            # We need to stringify some types to make them properly comparable
            if key in self.key_list:
                # numbers
                # 1000 -> +1.000000e+03 -> pp03e1.000000
                # -1000 -> -1/1000 -> -1.000000e-03 -> mm96e1.000000
                # 0 -> o
                if isinstance(value, (int, float, decimal.Decimal)):
                    if value:
                        parts = '{:+e}'.format(value if value >= 0 else 1/value).split('e')
                        power = int(parts[1])
                        value = '{}{}e{}'.format(
                            parts[0][0],
                            parts[1] if power >= 0 else str(-99 - power),
                            parts[0][1:])
                        value = value.replace('+', 'p').replace('-', 'm')
                    else:
                        value = 'o'
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
