import pytest

def test_exception_in_generator():
    from dataflows import Flow, printer

    class MyException(Exception):
        pass

    def generator():
        for i in range(5):
            raise MyException()
            yield {"i": i}

    with pytest.raises(MyException):
        Flow(generator(), printer()).process()
