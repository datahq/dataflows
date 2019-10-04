import os
import warnings
import datetime

from datapackage import Package
from tabulator import Stream
from tabulator.parser import Parser
from tabulator.helpers import reset_stream
from tableschema.schema import Schema
from .. import DataStreamProcessor
from ..base.schema_validator import schema_validator, ignore, drop, raise_exception
from ..helpers.resource_matcher import ResourceMatcher


class XMLParser(Parser):
    options = []

    def __init__(self, loader, force_parse, **options):
        self.__loader = loader
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__chars = None

    def open(self, source, encoding=None):
        self.close()
        self.__chars = self.__loader.load(source, encoding=encoding)
        self.__encoding = getattr(self.__chars, 'encoding', encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__chars.close()

    def reset(self):
        reset_stream(self.__chars)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def closed(self):
        return self.__chars is None or self.__chars.closed

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        from xml.etree.ElementTree import parse
        from xmljson import parker

        parsed = parker.data(parse(self.__chars).getroot())
        elements = list(parsed.values())
        if len(elements) > 0:
            elements = elements[0]
        else:
            elements = []
        for row_number, row in enumerate(elements, start=1):
            keys, values = zip(*(row.items()))
            yield (row_number, list(keys), list(values))


class StringsGuesser():
    def cast(self, value):
        return [('string', 'default', 0)]


class TypesGuesser():
    def cast(self, value):
        jts_type = {
            str: 'string',
            int: 'integer',
            float: 'number',
            list: 'array',
            dict: 'object',
            tuple: 'array',
            bool: 'boolean',
            datetime.datetime: 'datetime',
            datetime.date: 'date',
        }.get(type(value))
        ret = [('any', 'default', 0)]
        if jts_type is not None:
            ret.append(('jts_type', 'default', 1))
        return ret


class load(DataStreamProcessor):

    INFER_STRINGS = 'strings'
    INFER_PYTHON_TYPES = 'pytypes'
    INFER_FULL = 'full'

    CAST_TO_STRINGS = 'strings'
    CAST_DO_NOTHING = 'nothing'
    CAST_WITH_SCHEMA = 'schema'

    ERRORS_IGNORE = ignore
    ERRORS_DROP = drop
    ERRORS_RAISE = raise_exception

    def __init__(self, load_source, name=None, resources=None, strip=True, limit_rows=None,
                 infer_strategy=None, cast_strategy=None,
                 override_schema=None, override_fields=None,
                 deduplicate_headers=False,
                 on_error=raise_exception,
                 **options):
        super(load, self).__init__()
        self.load_source = load_source

        self.name = name
        self.strip = strip
        self.limit_rows = limit_rows
        self.options = options
        self.resources = resources
        self.override_schema = override_schema
        self.override_fields = override_fields
        self.deduplicate_headers = deduplicate_headers

        self.load_dp = None
        self.resource_descriptors = []
        self.iterators = []

        if 'force_strings' in options:
            warnings.warn('force_strings is being deprecated, use infer_strategy & cast_strategy instead',
                          DeprecationWarning)
            if options['force_strings']:
                infer_strategy = self.INFER_STRINGS
                cast_strategy = self.CAST_TO_STRINGS

        if 'validate' in options:
            warnings.warn('validate is being deprecated, use cast_strategy & on_error instead',
                          DeprecationWarning)
            if options['validate']:
                cast_strategy = self.CAST_WITH_SCHEMA

        self.guesser = {
            self.INFER_FULL: None,
            self.INFER_PYTHON_TYPES: TypesGuesser,
            self.INFER_STRINGS: StringsGuesser,
        }[infer_strategy or self.INFER_FULL]

        self.caster = {
            self.CAST_DO_NOTHING: lambda res, it: it,
            self.CAST_WITH_SCHEMA: lambda res, it: schema_validator(res, it, on_error=on_error),
            self.CAST_TO_STRINGS: lambda res, it: self.stringer(it)
        }[cast_strategy or self.CAST_DO_NOTHING]

    def process_datapackage(self, dp: Package):

        # If loading from datapackage & resource iterator:
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resource_iterator = self.load_source
            resources = datapackage_descriptor['resources']
            resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor['resources']:
                if resource_matcher.match(resource_descriptor['name']):
                    self.resource_descriptors.append(resource_descriptor)
            self.iterators = (resource for resource, descriptor in zip(resource_iterator, resources)
                              if resource_matcher.match(descriptor['name']))

        # If load_source is string:
        else:
            # Handle Environment vars if necessary:
            if self.load_source.startswith('env://'):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")

            # Loading from datapackage:
            if os.path.basename(self.load_source) == 'datapackage.json':
                self.load_dp = Package(self.load_source)
                resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                for resource in self.load_dp.resources:
                    if resource_matcher.match(resource.name):
                        self.resource_descriptors.append(resource.descriptor)
                        self.iterators.append(resource.iter(keyed=True, cast=True))

            # Loading for any other source
            else:
                path = os.path.basename(self.load_source)
                path = os.path.splitext(path)[0]
                descriptor = dict(path=self.name or path,
                                  profile='tabular-data-resource')
                self.resource_descriptors.append(descriptor)
                descriptor['name'] = self.name or path
                if 'encoding' in self.options:
                    descriptor['encoding'] = self.options['encoding']
                self.options.setdefault('custom_parsers', {}).setdefault('xml', XMLParser)
                self.options.setdefault('ignore_blank_headers', True)
                self.options.setdefault('headers', 1)
                stream: Stream = Stream(self.load_source, **self.options).open()
                if len(stream.headers) != len(set(stream.headers)):
                    if not self.deduplicate_headers:
                        raise ValueError(
                            'Found duplicate headers.' +
                            'Use the `deduplicate_headers` flag (found headers=%r)' % stream.headers)
                    stream.headers = self.rename_duplicate_headers(stream.headers)
                schema = Schema().infer(
                    stream.sample, headers=stream.headers,
                    confidence=1, guesser_cls=self.guesser)
                if self.override_schema:
                    schema.update(self.override_schema)
                if self.override_fields:
                    fields = schema.get('fields', [])
                    for field in fields:
                        field.update(self.override_fields.get(field['name'], {}))
                descriptor['schema'] = schema
                descriptor['format'] = self.options.get('format', stream.format)
                descriptor['path'] += '.{}'.format(stream.format)
                self.iterators.append(stream.iter(keyed=True))
        dp.descriptor.setdefault('resources', []).extend(self.resource_descriptors)
        return dp

    def stripper(self, iterator):
        for r in iterator:
            yield dict(
                (k, v.strip()) if isinstance(v, str) else (k, v)
                for k, v in r.items()
            )

    def limiter(self, iterator):
        count = 0
        for row in iterator:
            yield row
            count += 1
            if count >= self.limit_rows:
                break

    def stringer(self, iterator):
        for r in iterator:
            yield dict(
                (k, str(v)) if not isinstance(v, str) else (k, v)
                for k, v in r.items()
            )

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        for descriptor, it in zip(self.resource_descriptors, self.iterators):
            it = self.caster(descriptor, it)
            if self.strip:
                it = self.stripper(it)
            if self.limit_rows:
                it = self.limiter(it)
            yield it

    def rename_duplicate_headers(self, duplicate_headers):
        counter = {}
        headers = []
        for header in duplicate_headers:
            counter.setdefault(header, 0)
            counter[header] += 1
            if counter[header] > 1:
                if counter[header] == 2:
                    headers[headers.index(header)] = '%s (%s)' % (header, 1)
                header = '%s (%s)' % (header, counter[header])
            headers.append(header)
        return headers
