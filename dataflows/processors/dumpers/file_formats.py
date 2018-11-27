import csv
import json
import os
import isodate
import logging


DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIME_FORMAT = '%H:%M:%S'


def identity(x):
    return x


def json_dumps(x):
    return json.dumps(x, ensure_ascii=False)


class FileFormat():

    PYTHON_DIALECT = {}
    NULL_VALUE = None
    SERIALIZERS = {}
    DEFAULT_SERIALIZER = str

    def __init__(self, writer, schema):
        self.writer = writer
        self.headers = [f.name for f in schema.fields]
        self.fields = dict((f.name, f) for f in schema.fields)

    @classmethod
    def prepare_resource(cls, resource):
        for field in resource.descriptor['schema']['fields']:
            field.update(cls.PYTHON_DIALECT.get(field['type'], {}))

    def __transform_row(self, row):
        try:
            return dict((k, self.__transform_value(v, self.fields[k].type))
                        for k, v in row.items())
        except Exception:
            logging.exception('Failed to transform row %r', row)
            raise

    @classmethod
    def __transform_value(cls, value, field_type):
        if value is None:
            return cls.NULL_VALUE
        serializer = cls.SERIALIZERS.get(field_type, cls.DEFAULT_SERIALIZER)
        return serializer(value)

    def write_transformed_row(self, *_):
        raise NotImplementedError()

    def write_row(self, row):
        transformed_row = self.__transform_row(row)
        self.write_transformed_row(transformed_row)


class CsvTitlesDictWriter(csv.DictWriter):
    def __init__(self, *args, **kwargs):
        self.fieldtitles = kwargs.pop('fieldtitles')
        super().__init__(*args, **kwargs)

    def writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldtitles))
        self.writerow(header)


class CSVFormat(FileFormat):

    SERIALIZERS = {
        'array': json_dumps,
        'object': json_dumps,
        'datetime': lambda d: d.strftime(DATETIME_FORMAT),
        'date': lambda d: d.strftime(DATE_FORMAT),
        'time': lambda d: d.strftime(TIME_FORMAT),
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: '{}, {}'.format(*d),
        'geojson': json.dumps,
        'year': lambda d: '{:04d}'.format(d),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    DEFAULT_SERIALIZER = str
    NULL_VALUE = ''

    PYTHON_DIALECT = {
        'number': {
            'decimalChar': '.',
            'groupChar': ''
        },
        'date': {
            'format': DATE_FORMAT
        },
        'time': {
            'format': TIME_FORMAT
        },
        'datetime': {
            'format': DATETIME_FORMAT
        },
    }

    def __init__(self, file, schema, use_titles=False):
        headers = [f.name for f in schema.fields]
        if use_titles:
            titles = [f.descriptor.get('title', f.name) for f in schema.fields]
            csv_writer = CsvTitlesDictWriter(file, headers, fieldtitles=titles)
        else:
            csv_writer = csv.DictWriter(file, headers)
        csv_writer.writeheader()
        super(CSVFormat, self).__init__(csv_writer, schema)

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(descriptor['path'])
        descriptor['path'] = basename + '.csv'
        descriptor['format'] = 'csv'
        descriptor['dialect'] = dict(
            lineTerminator='\r\n',
            delimiter=',',
            doubleQuote=True,
            quoteChar='"',
            skipInitialSpace=False
        )
        super(CSVFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        self.writer.writerow(transformed_row)

    def finalize_file(self):
        pass


class JSONFormat(FileFormat):

    SERIALIZERS = {
        'datetime': lambda d: d.strftime(DATETIME_FORMAT),
        'date': lambda d: d.strftime(DATE_FORMAT),
        'time': lambda d: d.strftime(TIME_FORMAT),
        'number': float,
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: list(map(float, d)),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    DEFAULT_SERIALIZER = identity
    NULL_VALUE = None

    PYTHON_DIALECT = {
        'date': {
            'format': DATE_FORMAT
        },
        'time': {
            'format': TIME_FORMAT
        },
        'datetime': {
            'format': DATETIME_FORMAT
        },
    }

    def __init__(self, file, schema):
        writer = file
        writer.write('[')
        writer.__first = True
        super(JSONFormat, self).__init__(writer, schema)

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        basename, _ = os.path.splitext(resource.source)
        descriptor['path'] = basename + '.json'
        descriptor['format'] = 'json'
        descriptor['mediatype'] = 'text/json'
        super(JSONFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        if not self.writer.__first:
            self.writer.write(',')
        else:
            self.writer.__first = False
        self.writer.write(json.dumps(transformed_row, sort_keys=True, ensure_ascii=True))

    def finalize_file(self):
        self.writer.write(']')
