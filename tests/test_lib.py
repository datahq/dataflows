from dataflows import Flow

data = [
    dict(x=1, y='a'),
    dict(x=2, y='b'),
    dict(x=3, y='c'),
]


def test_add_computed_field():
    from dataflows import add_computed_field
    f = Flow(
        data,
        add_computed_field([
            dict(source=['x', 'x'], target='xx', operation='multiply'),
            dict(target='f', operation='format', with_='{y} - {x}')
        ])
    )
    results, dp, stats = f.results()
    results = list(results[0])

    xx = [x['xx'] for x in results]
    f = [x['f'] for x in results]

    assert xx == [1, 4, 9]
    assert f == ['a - 1', 'b - 2', 'c - 3']


def test_add_metadata():
    from dataflows import add_metadata
    f = Flow(
        data,
        add_metadata(author='Adam Kariv')
    )
    _, dp, _ = f.results()
    assert dp.descriptor['author'] == 'Adam Kariv'


def test_delete_field():
    from dataflows import delete_fields
    f = Flow(
        data,
        delete_fields(['x'])
    )
    results, dp, _ = f.results()
    for i in results[0]:
        assert list(i.keys()) == ['y']
    assert dp.descriptor['resources'][0]['schema']['fields'] == \
        [dict(name='y', type='string', format='default')]


def test_find_replace():
    from dataflows import find_replace
    f = Flow(
        data,
        find_replace([dict(
            name='y',
            patterns=[
                dict(find='a', replace='Apple'),
                dict(find='b', replace='Banana'),
                dict(find='c', replace='Coconut'),
            ]
        )])
    )
    results, _, _ = f.results()
    y = [r['y'] for r in results[0]]
    assert y == ['Apple', 'Banana', 'Coconut']


def test_unpivot():
    from dataflows import unpivot
    f = Flow(
        data,
        unpivot(
            [
                dict(name='x',
                     keys=dict(
                         field='x-value'
                     )
                ),
                dict(name='y',
                     keys=dict(
                         field='y-value'
                     )
                ),
            ],
            [
                dict(
                    name='field',
                    type='string'
                )
            ],
            dict(
                name='the-value',
                type='any'
            )
        )
    )
    results, _, _ = f.results()
    assert results[0] == [
        dict(zip(['field', 'the-value'], r)) 
        for r in
        [
            ['x-value', 1],
            ['y-value', 'a'],
            ['x-value', 2],
            ['y-value', 'b'],
            ['x-value', 3],
            ['y-value', 'c'],
        ]
    ]


def test_concatenate():
    from dataflows import concatenate
    
    f = Flow(
        [
            {'a': 1, 'b': 2},
            {'a': 2, 'b': 3},
            {'a': 3, 'b': 4},
        ],
        [
            {'c': 4, 'd': 5},
            {'c': 5, 'd': 6},
            {'c': 6, 'd': 7},
        ],
        concatenate({
            'f1': ['a', 'c'],
            'f2': ['b', 'd']
        })
    )
    results, _, _ = f.results()
    d = [(r['f1'], r['f2']) for r in results[0]]
    assert d == list(zip(range(1,7), range(2, 8)))


def test_filter_rows():
    from dataflows import filter_rows
    
    f = Flow(
        [
            {'a': 1, 'b': 3},
            {'a': 2, 'b': 3},
            {'a': 1, 'b': 4},
            {'a': 2, 'b': 4},
        ],
        filter_rows(equals=[dict(a=1)]),
        filter_rows(not_equals=[dict(b=3)]),
    )
    results, _, _ = f.results()
    assert results[0][0] == dict(a = 1, b = 4)
    assert len(results[0]) == 1
    assert len(results) == 1
