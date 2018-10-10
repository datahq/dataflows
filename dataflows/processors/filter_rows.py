import operator

from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, conditions):
    for row in rows:
        if any(func(row[k], v) for func, k, v in conditions):
            yield row


def filter_rows(equals=tuple(), not_equals=tuple(), resources=None):

    conditions = [
        (operator.eq, k, v)
        for o in equals
        for k, v in o.items()
    ] + [
        (operator.ne, k, v)
        for o in not_equals
        for k, v in o.items()
    ]

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for r in package:
            if matcher.match(r.res.name):
                yield process_resource(r, conditions)
            else:
                yield r

    return func
