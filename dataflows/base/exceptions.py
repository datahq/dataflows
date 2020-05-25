class DataflowsException(Exception):
    pass


class ProcessorError(DataflowsException):

    def __init__(self, cause, *, processor_name, processor_object, processor_position):
        self.cause = cause
        self.processor_name = processor_name
        self.processor_object = processor_object
        self.processor_position = processor_position
        super().__init__(str(cause))
