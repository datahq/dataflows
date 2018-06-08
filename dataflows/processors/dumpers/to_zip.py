import zipfile

from .file_dumper import FileDumper


class ZipDumper(FileDumper):

    def __init__(self, out_file, **options):
        super(ZipDumper, self).__init__(options)
        out_filename = open(out_file, 'wb')
        self.zip_file = zipfile.ZipFile(out_filename, 'w')

    def write_file_to_output(self, filename, path):
        self.zip_file.write(filename, arcname=path,
                            compress_type=zipfile.ZIP_DEFLATED)

    def finalize(self):
        self.zip_file.close()
        super(ZipDumper, self).finalize()
