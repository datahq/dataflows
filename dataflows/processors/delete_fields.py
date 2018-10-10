from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, fields):
    for row in rows:
        for field in fields:
            del row[field]
        yield row


def delete_fields(fields, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        dp_resources = package.pkg.descriptor.get('resources', [])
        for resource in dp_resources:
            if matcher.match(resource['name']):
                dp_fields = resource['schema'].get('fields', [])
                field_names = [f['name'] for f in dp_fields]
                non_existing = [f for f in fields if f not in field_names]

                assert len(non_existing) == 0, \
                    "Can't find following field(s): %s" % '; '.join(non_existing)

                new_fields = list(
                    filter(lambda x: x['name'] not in fields, dp_fields))
                resource['schema']['fields'] = new_fields
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(resource, fields)

    return func
