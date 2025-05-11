from dataflows import PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher


def set_primary_key(primary_key, resources=None):

    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                if primary_key:
                    resource.setdefault('schema', {})['primaryKey'] = primary_key
                else:
                    resource.get('schema', {}).pop('primaryKey', None)
        yield package.pkg

        res_iter = iter(package)
        for r in res_iter:
            if matcher.match(r.res.name):
                yield r.it
            else:
                yield r

    return func
