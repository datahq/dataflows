import operator

from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, conditions):
    for row in rows:
        if any(func(row[k], v) for func, k, v in conditions):
            yield row


def filter_rows(equals=tuple(), not_equals=tuple(), resources=None):

    matcher = ResourceMatcher(resources)

    conditions = [
        (operator.eq, k, v)
        for o in equals
        for k, v in o.items()
    ] + [
        (operator.ne, k, v)
        for o in not_equals
        for k, v in o.items()
    ]

    def func(rows):
        if matcher.match(rows.res.name):
            return process_resource(rows, conditions)
        else:
            return rows

    return func
