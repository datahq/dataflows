from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, fields):
    fields = set(fields)
    for row in rows:
        row = dict(
            (k, v)
            for k, v in row.items()
            if k in fields
        )
        yield row


def select_fields(fields, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        dp_resources = package.pkg.descriptor.get('resources', [])
        for resource in dp_resources:
            if matcher.match(resource['name']):
                dp_fields = resource['schema'].get('fields', [])
                dp_fields = dict(
                    (f['name'], f)
                    for f in dp_fields
                )
                non_existing = [f for f in fields if f not in dp_fields]

                assert len(non_existing) == 0, \
                    "Can't find following field(s): %s" % '; '.join(non_existing)

                new_fields = [
                    dp_fields.get(f)
                    for f in fields
                ]
                resource['schema']['fields'] = new_fields
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(resource, fields)

    return func
