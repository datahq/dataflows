def add_metadata(**metadata):

    def func(package):
        package.pkg.descriptor.update(metadata)
        yield package.pkg
        yield from package

    return func
