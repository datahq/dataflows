import csv
import json
from pathlib import Path
import isodate
import logging
import datetime
from functools import partial
from dataflows.helpers.extended_json import (
    DATETIME_F_FORMAT, DATE_F_FORMAT, TIME_F_FORMAT,
    DATETIME_P_FORMAT, DATE_P_FORMAT, TIME_P_FORMAT,
)


def identity(x):
    return x


def json_dumps(x):
    return json.dumps(x, ensure_ascii=False)


class FileFormat():

    PYTHON_DIALECT = {}
    NULL_VALUE = None
    SERIALIZERS = {}

    def __init__(self, writer, schema, temporal_format_property=None, default_serializer=str):

        # Set properties
        self.writer = writer
        self.headers = [f.name for f in schema.fields]
        self.fields = dict((f.name, f) for f in schema.fields)
        self.temporal_format_property = temporal_format_property
        self.missing_values = schema.descriptor.get('missingValues', [])

        # Set fields' serializers
        for field in schema.fields:
            serializer = self.SERIALIZERS.get(field.type, default_serializer)
            if self.temporal_format_property:
                if field.type in ['datetime', 'date', 'time']:
                    format = field.descriptor.get(self.temporal_format_property, None)
                    if format:
                        strftime = getattr(datetime, field.type).strftime
                        serializer = partial(strftime, format=format)
            field.descriptor['serializer'] = serializer

    @classmethod
    def prepare_resource(cls, resource):
        for field in resource.descriptor['schema']['fields']:
            field.update(cls.PYTHON_DIALECT.get(field['type'], {}))

    def __transform_row(self, row):
        try:
            return dict((k, self.__transform_value(v, self.fields[k]))
                        for k, v in row.items())
        except Exception:
            logging.exception('Failed to transform row %r', row)
            raise

    def __transform_value(self, value, field):
        if value is None:
            return self.NULL_VALUE
        # It supports a `tableschema`'s mode of perserving missing values
        # https://github.com/frictionlessdata/tableschema-py#experimental
        if value in self.missing_values:
            return value
        return field.descriptor['serializer'](value)

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
        'datetime': lambda d: d.strftime(DATETIME_F_FORMAT),
        'date': lambda d: d.strftime(DATE_F_FORMAT),
        'time': lambda d: d.strftime(TIME_F_FORMAT),
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: '{}, {}'.format(*d),
        'geojson': json.dumps,
        'year': lambda d: '{:04d}'.format(d),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    NULL_VALUE = ''

    PYTHON_DIALECT = {
        'number': {
            'decimalChar': '.',
            'groupChar': ''
        },
        'date': {
            'format': DATE_P_FORMAT
        },
        'time': {
            'format': TIME_P_FORMAT
        },
        'datetime': {
            'format': DATETIME_P_FORMAT
        },
        'boolean': {
            'trueValues': ['True'],
            'falseValues': ['False']
        }
    }

    def __init__(self, file, schema, use_titles=False, **options):
        headers = [f.name for f in schema.fields]
        if use_titles:
            titles = [f.descriptor.get('title', f.name) for f in schema.fields]
            csv_writer = CsvTitlesDictWriter(file, headers, fieldtitles=titles)
        else:
            csv_writer = csv.DictWriter(file, headers)
        csv_writer.writeheader()
        super(CSVFormat, self).__init__(csv_writer, schema, **options)

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        descriptor['path'] = str(Path(descriptor['path']).with_suffix('.csv'))
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
        'datetime': lambda d: d.strftime(DATETIME_F_FORMAT),
        'date': lambda d: d.strftime(DATE_F_FORMAT),
        'time': lambda d: d.strftime(TIME_F_FORMAT),
        'number': float,
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: list(map(float, d)),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }

    NULL_VALUE = None

    PYTHON_DIALECT = {
        'date': {
            'format': DATE_P_FORMAT
        },
        'time': {
            'format': TIME_P_FORMAT
        },
        'datetime': {
            'format': DATETIME_P_FORMAT
        },
    }

    def __init__(self, file, schema, **options):
        self.initialize_file(file)
        super(JSONFormat, self).__init__(file, schema, default_serializer=identity, **options)

    def initialize_file(self, file):
        file.write('[')
        file.__first = True

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        descriptor['path'] = str(Path(descriptor['path']).with_suffix('.json'))
        descriptor['format'] = 'json'
        descriptor['mediatype'] = 'text/json'
        descriptor['profile'] = 'tabular-data-resource'
        super(JSONFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        if not self.writer.__first:
            self.writer.write(',')
        else:
            self.writer.__first = False
        self.writer.write(json.dumps(transformed_row, sort_keys=True, ensure_ascii=True))

    def finalize_file(self):
        self.writer.write(']')


class GeoJSONFormat(JSONFormat):

    def initialize_file(self, file):
        file.write('{"type": "FeatureCollection","features":')
        super(GeoJSONFormat, self).initialize_file(file)

    def write_transformed_row(self, transformed_row):
        properties = dict()
        geometry = None
        for k, v in transformed_row.items():
            if self.fields[k].type == 'geopoint':
                geometry = dict(
                    type='Point',
                    coordinates=v
                )
            elif self.fields[k].type == 'geojson':
                geometry = v
            else:
                properties[k] = v
        feature = dict(
            geometry=geometry,
            type='Feature',
            properties=properties
        )
        super(GeoJSONFormat, self).write_transformed_row(feature)

    def finalize_file(self):
        super(GeoJSONFormat, self).finalize_file()
        self.writer.write('}')
