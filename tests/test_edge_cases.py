import pytest


class MyException(Exception):
    pass


def test_exception_in_generator():
    from dataflows import Flow, printer, exceptions

    def generator():
        for i in range(5):
            raise MyException()
            yield {"i": i}

    with pytest.raises(exceptions.ProcessorError) as excinfo:
        Flow(generator(), printer()).process()
    assert isinstance(excinfo.value.cause, MyException)


def test_exception_information():
    from dataflows import Flow, load, exceptions
    flow = Flow(
        load('data/bad-path1.csv'),
    )
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        flow.results()
    assert str(excinfo.value.cause).startswith("Failed to load source 'data/bad-path1.csv' and options")
    assert str(excinfo.value.cause).endswith("[Errno 2] No such file or directory: 'data/bad-path1.csv'")
    assert excinfo.value.processor_name == 'load'
    assert excinfo.value.processor_object.load_source == 'data/bad-path1.csv'
    assert excinfo.value.processor_position == 1


def test_exception_information_multiple_processors_simple():
    from dataflows import Flow, load, exceptions
    flow = Flow(
        load('data/bad-path1.csv'),
        load('data/bad-path2.csv'),
    )
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        flow.results()
    assert str(excinfo.value.cause).startswith("Failed to load source 'data/bad-path1.csv' and options")
    assert str(excinfo.value.cause).endswith(": [Errno 2] No such file or directory: 'data/bad-path1.csv'")
    assert excinfo.value.processor_name == 'load'
    assert excinfo.value.processor_object.load_source == 'data/bad-path1.csv'
    assert excinfo.value.processor_position == 1


def test_exception_information_multiple_processors_last_errored():
    from dataflows import Flow, load, exceptions
    flow = Flow(
        load('data/academy.csv'),
        load('data/bad-path2.csv'),
    )
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        flow.results()
    assert str(excinfo.value.cause).startswith("Failed to load source 'data/bad-path2.csv' and options")
    assert str(excinfo.value.cause).endswith(": [Errno 2] No such file or directory: 'data/bad-path2.csv'")
    assert excinfo.value.processor_name == 'load'
    assert excinfo.value.processor_object.load_source == 'data/bad-path2.csv'
    assert excinfo.value.processor_position == 2


def test_exception_information_multiple_processors_function_error():
    from dataflows import Flow, load, exceptions

    def func(rows):
        for i, row in enumerate(rows):
            if i == 1:
                raise MyException('custom-error')
            yield row

    flow = Flow(
        load('data/academy.csv'),
        func
    )
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        flow.results()
    assert str(excinfo.value.cause) == 'custom-error'
    assert excinfo.value.processor_name == 'rows_processor'
    assert excinfo.value.processor_position == 2


def test_exception_information_multiple_processors_iterable_error():
    from dataflows import Flow, printer, exceptions

    def func():
        for i in range(10):
            if i == 1:
                raise MyException('custom-iterable-error')
            yield dict(a=i)

    flow = Flow(
        func(),
        printer()
    )
    with pytest.raises(exceptions.ProcessorError) as excinfo:
        flow.results()
    assert str(excinfo.value.cause) == 'custom-iterable-error'
    assert excinfo.value.processor_name == 'iterable_loader'
    assert excinfo.value.processor_position == 1

def test_fullouter_join_dump_different_keys():
    from dataflows import Flow, join, dump_to_path
    data1 = [
        {"col1": 1.531, "col2": "hello"},
        {"col1": 1.132, "col2": "goodbye"},
    ]
    data2 = [
        {"colA": 1.531, "colB": "123"},
        {"colA": 1.132, "colB": 1.132},
    ]
    f = Flow(
        data1,
        data2,
        join(
            "res_1",
            ["col1"],
            "res_2",
            ["colA"],
            {"col2": {"name": "col2", "aggregate": "first"}},
            mode="full-outer"
        ),
        dump_to_path(out_path='out/test_join_dump'),
    )
    f.results()


