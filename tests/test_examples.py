def test_example_1():
    from dataflows import Flow

    data = [
        {'data': 'Hello'},
        {'data': 'World'}
    ]


    def lowerData(row):
        row['data'] = row['data'].lower()

    f = Flow(
        data,
        lowerData
    )
    data, *_ = f.results()

    print(data)

    # [[{'data': 'hello'}, {'data': 'world'}]]


def test_example_2():
    from dataflows import Flow, load

    def titleName(row):
        row['name'] = row['name'].title()

    f = Flow(
        load('data/beatles.csv'),
        titleName
    )
    data, *_ = f.results()

    print(data)


def country_population():
    from xml.etree import ElementTree
    from urllib.request import urlopen
    page = urlopen('https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population').read()
    tree = ElementTree.fromstring(page)
    tables = tree.findall('.//table')
    for table in tables:
        if 'wikitable' in table.attrib.get('class', ''):
            rows = table.findall('.//tr')
            for row in rows:
                cells = row.findall('td')
                if len(cells) > 3:
                    name = cells[1].find('.//a').attrib.get('title')
                    population = cells[2].text
                    yield(dict(
                        name=name,
                        population=population
                    ))

def test_example_3():
    from dataflows import Flow

    f = Flow(
        country_population(),
    )
    data, *_ = f.results()

    print(data)

def test_example_4():
    from dataflows import Flow, set_type

    f = Flow(
        country_population(),
        set_type('population', type='number', groupChar=',')
    )
    data, dp, _ = f.results()

    print(data[0][:10])

def test_example_5():
    from dataflows import Flow, set_type, dump_to_path

    f = Flow(
        country_population(),
        set_type('population', type='number', groupChar=','),
        dump_to_path('out/country_population')
    )
    _ = f.process()


def test_example_6():
    from dataflows import Flow, set_type, dump_to_path

    def all_triplets():
        for a in range(1, 21):
            for b in range(a, 21):
                for c in range(b+1, 21):
                    yield dict(a=a, b=b, c=c)


    def filter_pythagorean_triplets(rows):
        for row in rows:
            if row['a']**2 + row['b']**2 == row['c']**2:
                yield row

    f = Flow(
        all_triplets(),
        set_type('a', type='integer'),
        set_type('b', type='integer'),
        set_type('c', type='integer'),
        filter_pythagorean_triplets,
        dump_to_path('out/pythagorean_triplets')
    )
    _ = f.process()

def test_validate():
    from dataflows import Flow, validate, set_type, printer, ValidationError

    def adder(row):
        row['a'] += 0.5
        row['a'] = str(row['a'])


    f = Flow(
        (dict(a=x) for x in range(10)),
        set_type('a', type='integer'),
        adder,
        validate(),
        printer()
    )
    try:
        _ = f.process()
        assert False
    except ValidationError:
        pass


def test_example_7():
    from dataflows import Flow, load, dump_to_path


    def add_is_guitarist_column(package):

        # Add a new field to the first resource
        package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
            name='is_guitarist',
            type='boolean'
        ))
        # Must yield the modified datapackage
        yield package.pkg

        # Now iterate on all resources
        resources = iter(package)
        beatles = next(resources)

        def f(row):
            row['is_guitarist'] = row['instrument'] == 'guitar'
            return row

        yield map(f, beatles)

    f = Flow(
        # Same one as above
        load('data/beatles.csv'),
        add_is_guitarist_column,
        dump_to_path('out/beatles_guitarists')
    )
    _ = f.process()

def test_example_75():
    from dataflows import Flow, load, dump_to_path


    def add_is_guitarist_column_to_schema(package):

        # Add a new field to the first resource
        package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
            name='is_guitarist',
            type='boolean'
        ))
        # Must yield the modified datapackage
        yield package.pkg
        yield from package

    def add_is_guitarist_column(row):
        row['is_guitarist'] = row['instrument'] == 'guitar'
        return row

    f = Flow(
        # Same one as above
        load('data/beatles.csv'),
        add_is_guitarist_column_to_schema,
        add_is_guitarist_column,
        dump_to_path('out/beatles_guitarists2')
    )
    _ = f.process()


def test_example_8():
    from dataflows import Flow, load, dump_to_path

    def find_double_winners(package):

        # Remove the emmies resource - we're going to consume it now
        package.pkg.remove_resource('emmies')
        # Must yield the modified datapackage
        yield package.pkg

        # Now iterate on all resources
        resources = iter(package)

        # Emmies is the first - read all its data and create a set of winner names
        emmy = next(resources)
        emmy_winners = set(
            map(lambda x: x['nominee'],
                filter(lambda x: x['winner'],
                       emmy))
        )

        # Oscars are next - filter rows based on the emmy winner set
        academy = next(resources)
        yield filter(lambda row: row['Winner'] and row['Name'] in emmy_winners,
                     academy)

    f = Flow(
        # Emmy award nominees and winners
        load('data/emmy.csv', name='emmies'),
        # Academy award nominees and winners
        load('data/academy.csv', encoding='utf8', name='oscars'),
        find_double_winners,
        dump_to_path('out/double_winners')
    )
    _ = f.process()

def test_example_9():
    from dataflows import Flow, load, dump_to_path, join, concatenate, filter_rows

    f = Flow(
        # Emmy award nominees and winners
        load('data/emmy.csv', name='emmies'),
        filter_rows(equals=[dict(winner=1)]),
        concatenate(dict(
                emmy_nominee=['nominee'],
            ),
            dict(name='emmies_filtered'),
            resources='emmies'),
        # Academy award nominees and winners
        load('data/academy.csv', encoding='utf8', name='oscars'),
        join('emmies_filtered', ['emmy_nominee'],  # Source resource
             'oscars', ['Name'],                   # Target resource
             full=False   # Don't add new fields, remove unmatched rows
        ),
        filter_rows(equals=[dict(Winner='1')]),
        dump_to_path('out/double_winners')
    )
    _ = f.process()

def test_rename_resource():
    from dataflows import Flow, printer, PackageWrapper, ResourceWrapper

    def rename(package: PackageWrapper):
        package.pkg.descriptor['resources'][0]['name'] = 'renamed'
        yield package.pkg
        res_iter = iter(package)
        first: ResourceWrapper = next(res_iter)
        yield first.it
        yield from package

    f = Flow(
        ({'a': x} for x in range(10)),
        rename,
        printer()
    )
    results, dp, stats = f.results()
    print(dp.descriptor)
    assert dp.descriptor['resources'][0]['name'] == 'renamed'


def test_rename_resource2():
    from dataflows import Flow, printer, update_resource

    f = Flow(
        ({'a': x} for x in range(10)),
        update_resource(None, name='renamed'),
        printer()
    )
    results, dp, stats = f.results()
    print(dp.descriptor)
    assert dp.descriptor['resources'][0]['name'] == 'renamed'

def test_cast_strings_none_excel():
    from dataflows import Flow, load, printer
    f = Flow(
        # Emmy award nominees and winners
        load(
            'data/cast_string_none.xlsx',
            name='res',
            format='xlsx',
            sheet=1,
            headers=1,
            cast_strategy='strings',
            infer_strategy='strings',
        ),
        printer(),
    )
    results, dp, stats = f.results()
    assert results[0][0]['Individual'] is None
