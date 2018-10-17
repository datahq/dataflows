from dataflows import Flow

data = [
    dict(x=1, y='a'),
    dict(x=2, y='b'),
    dict(x=3, y='c'),
]

def test_dump_to_sql():
    from dataflows import Flow, printer, dump_to_sql
    from sqlalchemy import create_engine

    f = Flow(
        data,
        printer(),
        dump_to_sql(dict(
                output_table={
                    'resource-name': 'res_1'
                }
            ),
            engine='sqlite:///test.db')
    )
    f.process()

    # Check validity
    engine = create_engine('sqlite:///test.db')
    result = list(dict(x) for x in engine.execute('select * from output_table'))
    assert result == data

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


def test_sort_rows():
    from dataflows import sort_rows
    
    f = Flow(
        [
            {'a': 1, 'b': 3},
            {'a': 2, 'b': 3},
            {'a': 3, 'b': 1},
            {'a': 4, 'b': 1},
        ],
        sort_rows(key='{b}{a}'),
    )
    results, _, _ = f.results()
    assert list(results[0]) == [
        {'a': 3, 'b': 1},
        {'a': 4, 'b': 1},
        {'a': 1, 'b': 3},
        {'a': 2, 'b': 3},
    ]


def test_sort_reverse_many_rows():
    from dataflows import sort_rows

    f = Flow(
        ({'a': i, 'b': i%5} for i in range(1000)),
        sort_rows(key='{b}{a}', reverse=True, batch_size=0),
    )
    results, _, _ = f.results()
    results = results[0]
    assert results[0:2] == [{'a': 999, 'b': 4}, {'a': 994, 'b': 4}]
    assert results[998:1000] == [{'a': 100, 'b': 0}, {'a': 0, 'b': 0}]


def test_duplicate():
    from dataflows import duplicate
    
    a = [
            {'a': 1, 'b': 3},
            {'a': 2, 'b': 3},
            {'a': 3, 'b': 1},
            {'a': 4, 'b': 1},
        ]

    f = Flow(
        a,
        duplicate(),
    )
    results, _, _ = f.results()
    assert list(results[0]) == a
    assert list(results[1]) == a


def test_duplicate_many_rows():
    from dataflows import duplicate

    f = Flow(
        ({'a': i, 'b': i} for i in range(1000)),
        duplicate(),
    )

    results, _, _ = f.results()
    assert len(results[0]) == 1000
    assert len(results[1]) == 1000

    f = Flow(
        ({'a': i, 'b': i} for i in range(10000)),
        duplicate(batch_size=0),
    )

    results, _, _ = f.results()
    assert len(results[0]) == 10000
    assert len(results[1]) == 10000


def test_flow_as_step():
    def upper(row):
        for k in row:
            row[k] = row[k].upper()

    def lower_first_letter(row):
        for k in row:
            row[k] = row[k][0].lower() + row[k][1:]

    text_processing_flow = Flow(upper, lower_first_letter)

    assert Flow([{'foo': 'bar'}], text_processing_flow).results()[0] == [[{'foo': 'bAR'}]]


def test_load_from_package():
    from dataflows import dump_to_path, load

    Flow(
        [{'foo': 'bar'}],
        dump_to_path('data/load_from_package')
    ).process()

    ds = Flow(
        load('data/load_from_package/datapackage.json')
    ).datastream()

    assert len(ds.dp.resources) == 1
    assert [list(res) for res in ds.res_iter] == [[{'foo': 'bar'}]]


def test_load_from_env_var():
    import os
    from dataflows import load, dump_to_path
    
    Flow(
        [{'foo': 'bar'}],
        dump_to_path('data/load_from_package')
    ).process()

    os.environ['MY_DATAPACKAGE'] = 'data/load_from_package/datapackage.json'
    results, dp, _ = Flow(
        load('env://MY_DATAPACKAGE')
    ).results()

    assert len(dp.resources) == 1
    assert results == [[{'foo': 'bar'}]]


def test_load_from_package_resource_matching():
    from dataflows import dump_to_path, load

    Flow(
        [{'foo': 'bar'}],
        [{'foo': 'baz'}],
        dump_to_path('data/load_from_package')
    ).process()

    ds = Flow(
        load('data/load_from_package/datapackage.json', resources=['res_2'])
    ).datastream()

    assert len(ds.dp.resources) == 1
    assert [list(res) for res in ds.res_iter] == [[{'foo': 'baz'}]]



def test_load_from_package_resources():
    from dataflows import load

    datapackage = {'resources': [{'name': 'my-resource-{}'.format(i),
                                  'path': 'my-resource-{}.csv'.format(i),
                                  'schema': {'fields': [{'name': 'foo', 'type': 'string'}]}} for i in range(2)]}
    resources = ((row for row in [{'foo': 'bar{}'.format(i)}, {'foo': 'baz{}'.format(i)}]) for i in range(2))

    data, dp, *_ = Flow(
        load((datapackage, resources), resources=['my-resource-1']),
    ).results()

    assert len(dp.resources) == 1
    assert dp.get_resource('my-resource-1').descriptor['path'] == 'my-resource-1.csv'
    assert data[0][1] == {'foo': 'baz1'}


def test_cache():
    import os
    import shutil
    from dataflows import cache

    stats = {'a': 0, 'foo': 0}

    def incr_stat(name):
        stats[name] += 1
        return stats[name]

    cache_path = '.cache/test_cache'
    expected_files = ['datapackage.json', 'res_1.csv', 'cached_resource.csv']

    shutil.rmtree(cache_path, ignore_errors=True)

    def load_data(resource_name):

        def processor(package):
            package.pkg.add_resource({'name': resource_name,
                                      'path': resource_name+'.csv',
                                      'schema': {'fields': [{'name': 'foo', 'type': 'integer'}]}})
            yield package.pkg
            yield from package
            yield ({'foo': incr_stat('foo')} for _ in range(20))

        return processor

    f = Flow(
        cache(
            cache(
                ({'a': incr_stat('a'), 'i': i} for i in range(10)),
                cache_path=cache_path + '/first_cache'
            ),
            load_data('cached_resource'),
            cache_path=cache_path
        )
    )

    for i in range(3):
        results, *_ = f.results()
        assert results == [[{'a': i+1, 'i': i} for i in range(10)],
                           [{'foo': i+1} for i in range(20)]], 'failed iteration {}'.format(i)

    assert stats['a'] == 10
    assert stats['foo'] == 20
    for f in expected_files:
        assert os.path.exists(cache_path + '/' + f)


def test_update_resource():
    from dataflows import Flow, printer, update_resource

    f = Flow(
        *[
            ({k: x} for x in range(10))
            for k in 'abcdef'
        ],
        update_resource(['res_1', 'res_3', 'res_5'], source='thewild'),
        printer()
    )
    results, dp, stats = f.results()
    print(dp.descriptor)
    assert dp.descriptor['resources'][0]['source'] == 'thewild'
    assert dp.descriptor['resources'][2]['source'] == 'thewild'
    assert dp.descriptor['resources'][4]['source'] == 'thewild'


def test_set_type_resources():
    from dataflows import Flow, set_type, validate

    f = Flow(
        [dict(a=str(i)) for i in range(10)],
        [dict(b=str(i)) for i in range(10)],
        [dict(c='0_' + str(i)) for i in range(10)],
        set_type('a', resources='res_[1]', type='integer'),
        set_type('b', resources=['res_2'], type='integer'),
        set_type('[cd]', resources=-1, type='number', groupChar='_'),
        validate()
    )
    results, dp, stats = f.results()
    print(dp.descriptor) 
    assert results[0][1]['a'] == 1
    assert results[1][3]['b'] == 3
    assert results[2][8]['c'] == 8.0


def test_dump_to_path_use_titles():
    from dataflows import Flow, dump_to_path, set_type
    import tabulator

    Flow(
        [{'hello': 'world', 'hola': 'mundo'}, {'hello': 'עולם', 'hola': 'عالم'}],
        *(set_type(name, resources=['res_1'], title=title) for name, title
          in (('hello', 'שלום'), ('hola', 'aloha'))),
        dump_to_path('data/dump_with_titles', use_titles=True)
    ).process()

    with tabulator.Stream('data/dump_with_titles/res_1.csv') as stream:
        assert stream.read() == [['שלום',   'aloha'],
                                 ['world',  'mundo'],
                                 ['עולם',   'عالم']]


def test_add_field():
    from dataflows import Flow, add_field
    f = Flow(
        (dict(a=i) for i in range(3)),
        add_field('b', 'string', 'b'),
        add_field('c', 'number'),
        add_field('d', 'boolean', title='mybool'),
    )
    results, dp, _ = f.results()
    assert results == [[
        {'a': 0, 'b': 'b', 'c': None, 'd': None}, 
        {'a': 1, 'b': 'b', 'c': None, 'd': None}, 
        {'a': 2, 'b': 'b', 'c': None, 'd': None}
    ]]
    assert dp.descriptor == \
         {'profile': 'data-package',
          'resources': [{'name': 'res_1',
                         'path': 'res_1.csv',
                         'profile': 'tabular-data-resource',
                         'schema': {'fields': [{'format': 'default',
                                                'name': 'a',
                                                'type': 'integer'},
                                               {'format': 'default',
                                                'name': 'b',
                                                'type': 'string'},
                                               {'format': 'default',
                                                'name': 'c',
                                                'type': 'number'},
                                               {'format': 'default',
                                                'name': 'd',
                                                'title': 'mybool',
                                                'type': 'boolean'}],
                                    'missingValues': ['']}}]}
