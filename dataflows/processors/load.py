import os

from datapackage import Package
from tabulator import Stream
from tabulator.parser import Parser
from tabulator.helpers import reset_stream
from tableschema.schema import Schema
from .. import DataStreamProcessor
from ..base.schema_validator import schema_validator
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


class load(DataStreamProcessor):

    def __init__(self, load_source, name=None, resources=None, validate=False, strip=True, **options):
        super(load, self).__init__()
        self.load_source = load_source

        self.name = name
        self.validate = validate
        self.strip = strip
        self.options = options

        self.resources = resources

        self.load_dp = None
        self.force_strings = options.get('force_strings') is True
        self.resource_descriptors = []
        self.iterators = []

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
                        self.iterators.append(resource.iter(keyed=True, cast=False))

            # Loading for any other source
            else:
                path = os.path.basename(self.load_source)
                path = os.path.splitext(path)[0]
                descriptor = dict(path=path,
                                  profile='tabular-data-resource')
                self.resource_descriptors.append(descriptor)
                descriptor['name'] = self.name or path
                if 'encoding' in self.options:
                    descriptor['encoding'] = self.options['encoding']
                self.options.setdefault('custom_parsers', {}).setdefault('xml', XMLParser)
                self.options.setdefault('ignore_blank_headers', True)
                self.options.setdefault('headers', 1)
                stream: Stream = Stream(self.load_source, **self.options).open()
                descriptor['schema'] = Schema().infer(stream.sample, headers=stream.headers, confidence=1)
                if self.force_strings:
                    for f in descriptor['schema']['fields']:
                        f['type'] = 'string'
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

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        for descriptor, it in zip(self.resource_descriptors, self.iterators):
            if self.validate:
                it = schema_validator(descriptor, it)
            if self.strip:
                it = self.stripper(it)
            yield it
