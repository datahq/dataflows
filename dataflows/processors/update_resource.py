from dataflows import PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher


def update_resource(resource, **props):

    resources = ResourceMatcher(resource)

    def func(package: PackageWrapper):
        for resource in package.pkg.descriptor['resources']:
            if resources.match(resource['name']):
                resource.update(props)
        yield package.pkg

        res_iter = iter(package)
        for r in res_iter:
            if resources.match(r.res.name):
                yield r.it
            else:
                yield r

    return func
