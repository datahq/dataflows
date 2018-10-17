from dataflows.helpers.resource_matcher import ResourceMatcher


def column_adder(rows, k, v):
    for row in rows:
        row[k] = v
        yield row


def add_field(name, type, default=None, resources=None, **options):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                resource['schema']['fields'].append(dict(
                    name=name,
                    type=type,
                    **options
                ))
        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield column_adder(res, name, default)
            else:
                yield res

    return func
