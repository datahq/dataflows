import re

from ..helpers.resource_matcher import ResourceMatcher


def find_replace(fields, resources=None):

    matcher = ResourceMatcher(resources)

    def func(rows):
        if matcher.match(rows.res.name):
            for row in rows:
                for field in fields:
                    for pattern in field.get('patterns', []):
                        row[field['name']] = re.sub(
                            str(pattern['find']),
                            str(pattern['replace']),
                            str(row[field['name']]))
                yield row
        else:
            yield from rows

    return func
