import sys
import os

from ..helpers.extended_json import ejson


def stream(file=sys.stdout):

    if isinstance(file, str):
        basedir = os.path.dirname(file)
        os.makedirs(basedir, exist_ok=True)
        file = open(file, 'w')

    def write(obj):
        file.write(ejson.dumps(obj, sort_keys=True, ensure_ascii=True)+'\n')
        file.flush()

    def res_writer(res):
        for r in res:
            write(r)
            yield r

    def func(package):
        write(package.pkg.descriptor)
        yield package.pkg
        for res in package:
            yield res_writer(res)
            file.write('\n')

    return func
