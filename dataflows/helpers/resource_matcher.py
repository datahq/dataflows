import re


class ResourceMatcher(object):

    def __init__(self, resources):
        self.resources = resources
        if resources is None:
            self.resources = None
        elif isinstance(self.resources, str):
            self.resources = re.compile('^' + self.resources + '$')
            self.re = True
        else:
            assert isinstance(self.resources, list)
            self.re = False

    def match(self, name):
        if self.resources is None:
            return True
        if self.re:
            return self.resources.match(name) is not None
        else:
            return name in self.resources
