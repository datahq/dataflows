# DataFlows

DataFlows is a novel and intuitive way of building data processing flows.

- It's built for medium-data processing - data that fits on your hard drive, but is too big to load in Excel or as-is into Python, and not big enough to require spinning up a Hadoop cluster...
- It's built upon the foundation of the Frictionless Data project - which means that all data prduced by these flows is easily reusable by others.

## QuickStart / Tutorial

Let's start with the traditional 'hello, world' example:

```python
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

# -->
# [
#   [
#     {'data': 'hello'}, 
#     {'data': 'world'}
#   ]
# ]
```

This very simple flow takes a list of `dict`s and applies a row processing function on each one of them.

We can load data from a file instead:

```python
from dataflows import Flow, load

# beatles.csv:
# name,instrument 
# john,guitar
# paul,bass
# george,guitar
# ringo,drums

def titleName(row):
    row['name'] = row['name'].title()

f = Flow(
      load('beatles.csv'),
      titleName
)
data, *_ = f.results()

print(data)

# -->
# [
#   [
#     {'name': 'John', 'instrument': 'guitar'}, 
#     {'name': 'Paul', 'instrument': 'bass'}, 
#     {'name': 'George', 'instrument': 'guitar'}, 
#     {'name': 'Ringo', 'instrument': 'drums'}
#   ]
# ]
```

The source file can be a CSV file, an Excel file or a Json file. You can use a local file name or a URL for a file hosted somewhere on the web.

Data sources can be generators and not just lists or files. Let's take as an example a very simple scraper:

```python
from dataflows import Flow

from xml.etree import ElementTree
from urllib.request import urlopen

# Get from Wikipedia the population count for each country
def country_population():
    # Read the Wikipedia page and parse it using etree
    page = urlopen('https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population').read()
    tree = ElementTree.fromstring(page)
    # Iterate on all tables, rows and cells
    for table in tree.findall('.//table'):
        if 'wikitable' in table.attrib.get('class', ''):
            for row in table.findall('tr'):
                cells = row.findall('td')
                if len(cells) > 3:
                    # If a matching row is found...
                    name = cells[1].find('.//a').attrib.get('title')
                    population = cells[2].text
                    # ... yield a row with the information
                    yield dict(
                        name=name,
                        population=population
                    )

f = Flow(
      country_population(),
)
data, *_ = f.results()

print(data)
# ---> 
# [
#   [
#     {'name': 'China', 'population': '1,391,090,000'}, 
#     {'name': 'India', 'population': '1,332,140,000'}, 
#     {'name': 'United States', 'population': '327,187,000'},
#     {'name': 'Indonesia', 'population': '261,890,900'},
#     ...
#   ]
# ]
```

This is nice, but we do prefer the numbers to be actual numbers and not strings.

In order to do that, let's simply define their type to be numeric:

```python
from dataflows import Flow, set_type

def country_population():
    # same as before
	...

f = Flow(
	country_population(),
    set_type('population', type='number', groupChar=',')
)
data, *_ = f.results()

print(data)
# -->
# [
#   [
#     {'name': 'China', 'population': Decimal('1391090000')}, 
#     {'name': 'India', 'population': Decimal('1332140000')}, 
#     {'name': 'United States', 'population': Decimal('327187000')}, 
#     {'name': 'Indonesia', 'population': Decimal('261890900')},
#     ...
#   ]
# ]

```

Data is automatically converted to the correct native Python type.

Apart from data-types, it's also possible to set other constraints to the data. If the data fails validation (or does not fit the assigned data-type) an exception will be thrown - making this method highly effective for validating data and ensuring data quality. 

What about large data files? In the above examples, the results are loaded into memory, which is not always preferrable or acceptable. In many cases, we'd like to store the results directly onto a hard drive - without having the machine's RAM limit in any way the amount of data we can process.

We do it by using _dump_ processors:

```python
from dataflows import Flow, set_type, dump_to_path

def country_population():
    # same as before
	...

f = Flow(
	country_population(),
    set_type('population', type='number', groupChar=','),
    dump_to_path('country_population')
)
*_ = f.process()

```

Running this code will create a local directory called `county_population`, containing two files:

```
├── country_population
│   ├── datapackage.json
│   └── res_1.csv
```

The CSV file - `res_1.csv` - is where the data is stored. The `datapackage.json` file is a metadata file, holding information about the data, including its schema.

We can now open the CSV file with any spreadsheet program or code library supporting the CSV format - or using one of the **data package** libraries out there, like so:

```python
from datapackage import Package
pkg = Package('country_population/res_1.csv')
it = pkg.resources[0].iter(keyed=True)
print(next(it))
# prints:
# {'name': 'China', 'population': Decimal('1391110000')}
```

Note how using the data package meta-data, data-types are restored and there's no need to 're-parse' the data. This also works with other types too, such as dates, booleans and even `list`s and `dict`s.

So far we've seen how to load data, process it row by row, and then inspect the results or store them in a data package.

Let's see how we can do more complex processing by manipulating the entire data stream:

```python
from dataflows import Flow, set_type, dump_to_path

# Generate all triplets (a,b,c) so that 1 <= a <= b < c <= 20
def all_triplets():
    for a in range(1, 20):
        for b in range(a, 20):
            for c in range(b+1, 21):
                yield dict(a=a, b=b, c=c)

# Yield row only if a^2 + b^2 == c^1
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
    dump_to_path('pythagorean_triplets')
)
_ = f.process()

# -->
# pythagorean_triplets/res_1.csv contains:
# a,b,c
# 3,4,5
# 5,12,13
# 6,8,10
# 8,15,17
# 9,12,15
# 12,16,20
```

The `filter_pythagorean_triplets` function takes an iterator of rows, and yields only the ones that pass its condition. 

The flow framework knows whether a function is meant to hande a single row or a row iterator based on its parameters: 

- if it accepts a single `row` parameter, then it's a row processor.
- if it accepts a single `rows` parameter, then it's a rows processor.
- if it accepts a single `package` parameter, then it's a package processor.

Let's see a few examples of what we can do with a package processors.

First, let's add a field to the data:

```python
from dataflows import Flow, load, dump_to_path


def add_is_guitarist_column_to_schema(package):
	# Add a new field to the first resource
    package.pkg.resources[0]
               .descriptor['schema']['fields']
               .append(dict(
            name='is_guitarist',
            type='boolean'
    ))
    # Must yield the modified datapackage
    yield package.pkg
    # And its resources
    yield from package

def add_is_guitarist_column(row):
	row['is_guitarist'] = row['instrument'] == 'guitar'
    return row

f = Flow(
    # Same one as above
    load('beatles.csv'),
    add_is_guitarist_column_to_schema,
    add_is_guitarist_column,
    dump_to_path('beatles_guitarists')
)
_ = f.process()

```

In this example we create two steps - one for adding the new field (`is_guitarist`) to the schema and another step to modify the actual data.

We can combine the two into one step:

```python
from dataflows import Flow, load, dump_to_path


def add_is_guitarist_column(package):

    # Add a new field to the first resource
    package.pkg.resources[0].descriptor['schema']['fields'].append(dict(
        name='is_guitarist',
        type='boolean'
    ))
    # Must yield the modified datapackage
    yield package.pkg

    # Now iterate on all resources
    resources = iter(package)
    # Take the first resource
    beatles = next(resources)

    # And yield it with with the modification
    def f(row):
        row['is_guitarist'] = row['instrument'] == 'guitar'
        return row

    yield map(f, beatles)

f = Flow(
    # Same one as above
    load('beatles.csv'),
    add_is_guitarist_column,
    dump_to_path('beatles_guitarists')
)
_ = f.process()
```

The contract for the `package` processing function is simple:

First modify `package.pkg` (which is a `Package` instance) and yield it.

Then, yield any resources that should exist on the output, with or without modifications.

In the next example we're removing an entire resource in a package processor - this next one filters the list of Academy Award nominees to those who won both the Oscar and an Emmy award:

```python
    from dataflows import Flow, load, dump_to_path

    def find_double_winners(package):

        # Remove the emmies resource - 
        #    we're going to consume it now
        package.pkg.remove_resource('emmies')
        # Must yield the modified datapackage
        yield package.pkg

        # Now iterate on all resources
        resources = iter(package)

        # Emmies is the first - 
        # read all its data and create a set of winner names
        emmy = next(resources)
        emmy_winners = set(
            map(lambda x: x['nominee'], 
                filter(lambda x: x['winner'],
                       emmy))
        )

        # Oscars are next - 
        # filter rows based on the emmy winner set
        academy = next(resources)
        yield filter(lambda row: (row['Winner'] and 
                                  row['Name'] in emmy_winners),
                     academy)

    f = Flow(
        # Emmy award nominees and winners
        load('emmy.csv', name='emmies'),
        # Academy award nominees and winners
        load('academy.csv', encoding='utf8', name='oscars'),
        find_double_winners,
        dump_to_path('double_winners')
    )
    _ = f.process()

# --> 
# double_winners/academy.csv contains:
# 1931/1932,5,Actress,1,Helen Hayes,The Sin of Madelon Claudet
# 1932/1933,6,Actress,1,Katharine Hepburn,Morning Glory
# 1935,8,Actress,1,Bette Davis,Dangerous
# 1938,11,Actress,1,Bette Davis,Jezebel
# ...
```

## Builtin Processors

DataFlows comes with a few built-in processors which do most of the heavy lifting in many common scenarios - 
leaving you to implement only the minimum code that is specific to your specific problem.

### Load and Save Data
#### load
Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)

#### printer
Just prints whatever it sees. Good for debugging.

#### dump_to_path
Store the results to a specified path on disk, in a valid datapackage

#### dump_to_zip
Store the results in a valid datapackage, all files archived in one zip file

#### dump_to_sql
Store the results in a relational database (creates one or more tables or updates existing tables)

### Manipulate row-by-row
#### delete_fields.py
Removes some columns for the data

#### add_computed_field
Adds new fields whose values are based on existing columns

#### find_replace.py
Look for specific patterns in specific fields and replace them with new data

#### set_type.py
Parse incoming data based on provided schema, validate the data in the process
 
### Manipulate the entire resource
#### sort_rows.py
Sort incoming data based on key

#### unpivot.py
Unpivot a table - convert one row with multiple value columns to multiple rows with one value column

#### filter_rows.py
Filter rows based on inclusive and exclusive value filters

### Manipulate package
#### add_metadata.py
Add high-level metadata about your package

#### concatenate.py
Concatenate multiple streams of data to a single one, resolving differently named columns along the way

#### duplicate.py
Duplicate a single stream of data to make two streams
