def noop():
    """Pass through package unchanged."""

    def func(package):
        yield package.pkg
        yield from package

    return func
