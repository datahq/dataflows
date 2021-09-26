from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows.processors.add_computed_field import get_new_fields


def unwind(from_key: str, to_key: dict, transformer=None, resources=None, source_delete=True):

    """From a row of data, generate a row per value from from_key, where the value is set onto to_key."""

    def _unwinder(rows):
        for row in rows:
            try:
                iter(row[from_key])
                for value in row[from_key]:
                    ret = {}
                    ret.update(row)
                    ret[to_key['name']] = value if transformer is None else transformer(value)
                    if source_delete is True:
                        del ret[from_key]
                    yield ret
            except TypeError:
                # no iterable to unwind. Take the value we have and set it on the to_key.
                ret = {}
                ret.update(row)
                ret[to_key['name']] = (
                    ret[from_key] if transformer is None else transformer(ret[from_key])
                )
                if source_delete is True:
                    del ret[from_key]
                yield ret

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                new_fields = get_new_fields(
                    resource, [{'target': {'name': to_key['name'], 'type': to_key['type']}}]
                )
                if source_delete is True:
                    resource['schema']['fields'] = [
                        field
                        for field in resource['schema']['fields']
                        if not field['name'] == from_key
                    ]
                resource['schema']['fields'].extend(new_fields)

        yield package.pkg

        for resource in package:
            if matcher.match(resource.res.name):
                yield _unwinder(resource)
            else:
                yield resource

    return func
