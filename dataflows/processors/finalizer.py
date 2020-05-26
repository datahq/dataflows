def finalizer(callback):
    def func(package):
        yield package.pkg
        yield from package
        callback()
    return func
