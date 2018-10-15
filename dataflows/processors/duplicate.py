import copy
from kvfile import KVFile


def saver(resource, db, batch_size):
    gen = db.insert_generator(
        (("{:08x}".format(idx), row)
         for idx, row
         in enumerate(resource)),
        batch_size=batch_size
    )
    for _, row in gen:
        yield row


def loader(db):
    for _, value in db.items():
        yield value


def duplicate(source=None, target_name=None, target_path=None, batch_size=1000):
    def func(package):
        source_, target_name_, target_path_ = source, target_name, target_path
        if source_ is None:
            source_ = package.pkg.descriptor['resources'][0]['name']
        if target_name_ is None:
            target_name_ = source_ + '_copy'
        if target_path is None:
            target_path_ = target_name_ + '.csv'

        def traverse_resources(resources):
            for res in resources:
                yield res
                if res['name'] == source_:
                    res = copy.deepcopy(res)
                    res['name'] = target_name_
                    res['path'] = target_path_
                    yield res

        descriptor = package.pkg.descriptor
        descriptor['resources'] = list(traverse_resources(descriptor['resources']))
        yield package.pkg

        for resource in package:
            if resource.res.name == source_:
                db = KVFile()
                yield saver(resource, db, batch_size)
                yield loader(db)
            else:
                yield resource

    return func
