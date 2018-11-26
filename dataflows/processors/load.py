import os

from datapackage import Package, Resource
from tabulator.parser import Parser
from tabulator.helpers import reset_stream
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
        self.options = options
        self.name = name
        self.resources = resources
        self.load_dp = None
        self.validate = validate
        self.strip = strip
        self.force_strings = options.get('force_strings') is True

    def process_datapackage(self, dp: Package):
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, _ = self.load_source
            dp.descriptor.setdefault('resources', [])
            self.resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor['resources']:
                if self.resource_matcher.match(resource_descriptor['name']):
                    dp.add_resource(resource_descriptor)
        else:  # load_source is string:
            if self.load_source.startswith('env://'):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")
            if os.path.basename(self.load_source) == 'datapackage.json':
                self.load_dp = Package(self.load_source)
                self.resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                dp.descriptor.setdefault('resources', [])
                for resource in self.load_dp.resources:
                    if self.resource_matcher.match(resource.name):
                        dp.add_resource(resource.descriptor)
            else:
                if os.path.exists(self.load_source):
                    base_path = os.path.dirname(self.load_source) or '.'
                    self.load_source = os.path.basename(self.load_source)
                else:
                    base_path = None
                descriptor = dict(path=self.load_source,
                                  profile='tabular-data-resource')
                descriptor['format'] = self.options.get('format')
                if 'encoding' in self.options:
                    descriptor['encoding'] = self.options['encoding']
                if descriptor['format'] == 'xml' or self.load_source.endswith('.xml'):
                    self.options.setdefault('custom_parsers', {})['xml'] = XMLParser
                self.options.setdefault('ignore_blank_headers', True)
                self.options.setdefault('headers', 1)
                self.res = Resource(descriptor,
                                    base_path=base_path,
                                    **self.options)
                self.res.infer(confidence=1, limit=1000)
                if self.name is not None:
                    self.res.descriptor['name'] = self.name
                if self.force_strings:
                    for f in self.res.descriptor['schema']['fields']:
                        f['type'] = 'string'
                self.res.commit()
                self.res.descriptor['path'] = '{name}.{format}'.format(**self.res.descriptor)
                dp.add_resource(self.res.descriptor)
        return dp

    def stripper(self, iterator):
        for r in iterator:
            yield dict(
                (k, v.strip()) if isinstance(v, str) else (k, v)
                for k, v in r.items()
            )

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resources = self.load_source
            yield from (resource for resource, descriptor in zip(resources, datapackage_descriptor['resources'])
                        if self.resource_matcher.match(descriptor['name']))
        elif self.load_dp is not None:
            yield from (resource.iter(keyed=True) for resource in self.load_dp.resources
                        if self.resource_matcher.match(resource.name))
        else:
            it = self.res.iter(keyed=True, cast=False)
            if self.validate:
                it = schema_validator(self.res, it)
            if self.strip:
                it = self.stripper(it)
            yield it
