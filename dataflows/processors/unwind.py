from dataflows.helpers.resource_matcher import ResourceMatcher


def unwind(from_key, to_key, transformer=None, resources=None, source_delete=True):

    """From a row of data, generate a row per value from from_key, where the value is set onto to_key."""

    def _unwinder(rows):
        for row in rows:
            try:
                iter(row[from_key])
                for value in row[from_key]:
                    ret = {}
                    ret.update(row)
                    ret[to_key] = value if transformer is None else transformer(value)
                    if source_delete is True:
                        del ret[from_key]
                    yield ret
            except TypeError:
                # no iterable to unwind. Take the value we have and set it on the to_key.
                ret = {}
                ret.update(row)
                ret[to_key] = (
                    ret[from_key] if transformer is None else transformer(ret[from_key])
                )
                if source_delete is True:
                    del ret[from_key]
                yield ret

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for r in package:
            if matcher.match(r.res.name):
                yield _unwinder(r)
            else:
                yield r

    return func
