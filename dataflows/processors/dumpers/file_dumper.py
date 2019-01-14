import os
import json
import tempfile
import hashlib

from datapackage import Resource

from .dumper_base import DumperBase
from .file_formats import CSVFormat, JSONFormat


class FileDumper(DumperBase):

    def __init__(self, options):
        super(FileDumper, self).__init__(options)
        self.force_format = options.get('force_format', True)
        self.forced_format = options.get('format', 'csv')
        self.use_titles = options.get('use_titles', False)

    def process_datapackage(self, datapackage):
        datapackage = \
            super(FileDumper, self).process_datapackage(datapackage)

        self.file_formatters = {}

        # Make sure all resources are proper CSVs
        resource: Resource = None
        for i, resource in enumerate(datapackage.resources):
            if self.force_format:
                file_format = self.forced_format
            else:
                _, file_format = os.path.splitext(resource.source)
                file_format = file_format[1:]
            file_formatter = {
                'csv': CSVFormat,
                'json': JSONFormat
            }.get(file_format)
            if file_format is not None:
                self.file_formatters[resource.name] = file_formatter
                self.file_formatters[resource.name].prepare_resource(resource)
                resource.commit()
                datapackage.descriptor['resources'][i] = resource.descriptor

        return datapackage

    def handle_datapackage(self):
        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding='utf-8')
        indent = 2 if self.pretty_descriptor else None
        json.dump(self.datapackage.descriptor, temp_file, indent=indent, sort_keys=True, ensure_ascii=False)
        temp_file_name = temp_file.name
        filesize = temp_file.tell()
        temp_file.close()
        DumperBase.inc_attr(self.datapackage.descriptor, self.datapackage_bytes, filesize)
        self.write_file_to_output(temp_file_name, 'datapackage.json')
        # if location is not None:
        #     stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = location
        os.unlink(temp_file_name)
        super(FileDumper, self).handle_datapackage()

    def write_file_to_output(self, filename, path):
        raise NotImplementedError()

    def rows_processor(self, resource, writer, temp_file):
        for row in resource:
            writer.write_row(row)
            yield row
        writer.finalize_file()

        # File size:
        filesize = temp_file.tell()
        DumperBase.inc_attr(self.datapackage.descriptor, self.datapackage_bytes, filesize)
        DumperBase.inc_attr(resource.res.descriptor, self.resource_bytes, filesize)

        # File Hash:
        if self.resource_hash:
            hasher = FileDumper.hash_handler(temp_file)
            # Update path with hash
            if self.add_filehash_to_path:
                DumperBase.insert_hash_in_path(resource.res.descriptor, hasher.hexdigest())
            DumperBase.set_attr(resource.res.descriptor, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()
        self.write_file_to_output(filename, resource.res.source)
        os.unlink(filename)

    def process_resource(self, resource):
        if resource.res.name in self.file_formatters:
            schema = resource.res.schema

            temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, newline='')
            writer_kwargs = {'use_titles': True} if self.use_titles else {}
            writer = self.file_formatters[resource.res.name](temp_file, schema, **writer_kwargs)

            return self.rows_processor(resource,
                                       writer,
                                       temp_file)
        else:
            return resource

    @staticmethod
    def hash_handler(tfile):
        tfile.seek(0)
        hasher = hashlib.md5()
        data = 'x'
        while len(data) > 0:
            data = tfile.read(1024)
            if isinstance(data, str):
                hasher.update(data.encode('utf8'))
            elif isinstance(data, bytes):
                hasher.update(data)
        return hasher
