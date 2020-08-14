import jsonschema
from .. import DataStreamProcessor
from ..base.schema_validator import raise_exception, wrap_handler
from ..helpers import ResourceMatcher


class validate_metadata(DataStreamProcessor):

    def __init__(self, profile, resources=None, on_error=None):
        super().__init__()
        if on_error is None:
            on_error = raise_exception
        self.on_error = wrap_handler(on_error)
        self.resources = resources
        assert isinstance(profile, dict), 'Profile must be a dict'
        self.validator = self.resource_validator(profile)

    def resource_validator(self, profile):
        validator_class = jsonschema.validators.validator_for(profile)
        validator = validator_class(profile)
        errors = []
        def func(resource):
            for error in validator.iter_errors(resource.descriptor):
                metadata_path = "/".join(map(str, error.path))
                profile_path = "/".join(map(str, error.schema_path))
                note = '"%s" at "%s" in metadata and at "%s" in profile'
                note = note % (error.message, metadata_path, profile_path)
                errors.append(note)
            return errors
        return func

    def process_datapackage(self, dp):
        self.resources = ResourceMatcher(self.resources, dp)
        for resource, descriptor in zip(dp.resources, dp.descriptor['resources']):
            if self.resources.match(resource.name):
                errors = self.validator(resource)
                descriptor['errors'] = errors
        dp.commit()
        return super().process_datapackage(dp)
