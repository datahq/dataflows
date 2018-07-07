# ![logo](/Users/adam/code/datastream/logo-s.png) ~DataFlows~ 

DataFlows is a novel and intuitive way of building data processing flows.

- It's built for medium-data processing - data that fits on your hard drive, but is too big to load in Excel or as-is into Python, and not big enough to require spinning up a Hadoop cluster...
- It's built upon the foundation of the Frictionless Data project - which means that all data prduced by these flows is easily reusable by others.

## QuickStart 

Install `dataflows` via `pip install.`

Then use the command-line interface to bootstrap a basic processing script based on your needs:

```bash

$ pip install dataflows

$ dataflows
Hi There!
DataFlows will now bootstrap a data processing flow based on your needs.

Press any key to start...
...

```

## Tutorial

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

DataFlows comes with a few built-in processors which do most of the heavy lifting in many common scenarios, leaving you to implement only the minimum code that is specific to your specific problem.

### Load and Save Data
- **load** - Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)
- **printer** - Just prints whatever it sees. Good for debugging.

- **dump_to_path** - Store the results to a specified path on disk, in a valid datapackage
- **dump_to_zip** - Store the results in a valid datapackage, all files archived in one zipped file
- **dump_to_sql** - Store the results in a relational database (creates one or more tables or updates existing tables)

### Manipulate row-by-row
- **delete_fields** - Removes some columns from the data
- **add_computed_field** - Adds new fields whose values are based on existing columns
- **find_replace** - Look for specific patterns in specific fields and replace them with new data
- **set_type** - Parse incoming data based on provided schema, validate the data in the process

### Manipulate the entire resource
- **sort_rows** - Sort incoming data based on key
- **unpivot** - Unpivot a table - convert one row with multiple value columns to multiple rows with one value column
 - **filter_rows** - Filter rows based on inclusive and exclusive value filters

### Manipulate package
- **add_metadata** - Add high-level metadata about your package
- **concatenate** - Concatenate multiple streams of data to a single one, resolving differently named columns along the way
- **duplicate** - Duplicate a single stream of data to make two streams

### API Reference

#### load
Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)

```python
def load(path, name=None, **options):
    pass
```

- `path` - location of the data that is to be loaded. This can be either:
    - a local path (e.g. `/path/to/the/data.csv`)
    - a remote URL (e.g. `https://path.to/the/data.csv`)
    - Other supported links, based on the current support of schemes and formats in [tabulator](https://github.com/frictionlessdata/tabulator-py#schemes)
- `options` - based on the loaded file, extra options (e.g. `sheet` for Excel files etc., see the link to tabulator above)

#### printer
Just prints whatever it sees. Good for debugging.

#### dump_to_path
Store the results to a specified path on disk, in a valid datapackage

```python
def dump_to_path(out_path='.', 
                 force_format=True, format='csv', 
                 counters={}, 
                 add_filehash_to_path=False, 	
                 pretty_Descriptor=True):
    pass
```

- `out-path` - Name of the output path where `datapackage.json` will be stored.

  This path will be created if it doesn't exist, as well as internal data-package paths.

  If omitted, then `.` (the current directory) will be assumed.

- `force-format` - Specifies whether to force all output files to be generated with the same format
    - if `True` (the default), all resources will use the same format
    - if `False`, format will be deduced from the file extension. Resources with unknown extensions will be discarded.
- `format` - Specifies the type of output files to be generated (if `force-format` is true): `csv` (the default) or `json`
- `add-filehash-to-path`: Specifies whether to include file md5 hash into the resource path. Defaults to `False`. If `True` Embeds hash in path like so:
    - If original path is `path/to/the/file.ext`
    - Modified path will be `path/to/the/HASH/file.ext`
- `counters` - Specifies whether to count rows, bytes or md5 hash of the data and where it should be stored. An object with the following properties:

    | Key                  | Purpose                                | Default       |
    | -------------------- | -------------------------------------- | ------------- |
    | datapackage-rowcount | the # of rows in the entire package    | count_of_rows |
    | datapackage-bytes    | the # of bytes in the entire package   | bytes         |
    | datapackage-hash     | hash of the data in the entire package | hash          |
    | resource-rowcount    | the # of rows in each resource         | count_of_rows |
    | resource-bytes       | the # of bytes in each resource        | bytes         |
    | resource-hash        | hash of the data in the resource       | hash          |

    Each of these attributes could be set to null in order to prevent the counting.
    Each property could be a dot-separated string, for storing the data inside a nested object (e.g. `stats.rowcount`)
- `pretty-descriptor`: Specifies how datapackage descriptor (`datapackage.json`) file will look like:
    - `False` - descriptor will be written in one line.
    - `True` (default) - descriptor will have indents and new lines for each key, so it becomes more human-readable.


- `add_filehash_to_path` - Should paths for data files be injected with the data hash (i.e. instead of `path/to/data.csv`, have `path/to/<data.csv hash>/data.csv`) (default: `False`)

- `pretty_descriptor` - Should the resulting descriptor be JSON pretty-formatted with indentation for readability, or as compact as possible (default `True`)

#### dump_to_zip
Store the results in a valid datapackage, all files archived in one zipped file

```python
def dump_to_zip(out_file, 
                force_format=True, format='csv', 
                counters={}, 
                add_filehash_to_path=False, pretty_Descriptor=True):
    pass
```

- `out_file` - the path of the output zip file

(rest of parameters are detailed in `dump_to_path`)
#### dump_to_sql
Store the results in a relational database (creates one or more tables or updates existing tables)

### Manipulate row-by-row
#### delete_fields
Delete fields (columns) from streamed resources

`delete_fields` accepts a list of resources and list of fields to remove

_Note: if multiple resources provided, all of them should contain all fields to delete_

```python
def delete_fields(fields, resources=None):
    pass
```

- `fields` - List of field (column) names to be removed
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources

#### add_computed_field
Add field(s) to streamed resources

`add_computed_field` accepts a list of resources and fields to add to existing resource. It will output the rows for each resource with new field(s) (columns) in it. `add_computed_field` allows to perform various operations before inserting value into targeted field.

Adds new fields whose values are based on existing columns

```python
def add_computed_field(fields, resources=None):
    pass
```

- `fields` - List of actions to be performed on the targeted fields. Each list item is an object with the following keys:
  - `operation`: operation to perform on values of pre-defined columns of the same row. available operations:
    - `constant` - add a constant value
    - `sum` - summed value for given columns in a row.
    - `avg` - average value from given columns in a row.
    - `min` - minimum value among given columns in a row.
    - `max` - maximum value among given columns in a row.
    - `multiply` - product of given columns in a row.
    - `join` - joins two or more column values in a row.
    - `format` - Python format string used to form the value Eg:  `my name is {first_name}`.
  - `target` - name of the new field.
  - `source` - list of columns the operations should be performed on (Not required in case of `format` and `constant`).
  - `with` - String passed to `constant`, `format` or `join` operations
    - in `constant` - used as constant value
    - in `format` - used as Python format string with existing column values Eg: `{first_name} {last_name}`
    - in `join` - used as delimiter
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources


#### find_replace.py
Look for specific patterns in specific fields and replace them with new data

```python
def find_replace(fields, resources=None):
    pass
```

- `fields`- list of fields to replace values in. Each list item is an object with the following keys:
  - `name` - name of the field to replace value
  - `patterns` - list of patterns to find and replace from field
    - `find` - String, interpreted as a regular expression to match field value
    - `replace` - String, interpreted as a regular expression to replace matched pattern
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources

#### set_type.py
Sets a field's data type and type options and validates its data based on its new type definition.

This processor modifies the last resource in the package.

```python
def set_Type(name, **options):
    pass
```

- `name` - the name of the field to modify
- `options` - options to set for the field. Most common ones would be:
  - `type` - set the data type (e.g. `string`, `integer`, `number` etc.)
  - `format` - e.g. for date fields 
  etc.
 (more info on possible options can be found in the [tableschema spec](https://frictionlessdata.io/specs/table-schema/))

### Manipulate the entire resource
#### sort_rows.py
Sort incoming data based on key.

`sort_rows` accepts a list of resources and a key (as a Python format string on row fields).
It will output the rows for each resource, sorted according to the key (in ascending order).


```python
def sort_rows(key, resources=None):
    pass
```

- `key` - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources

#### unpivot.py
Unpivot a table - convert one row with multiple value columns to multiple rows with one value column

```python
def unpivot(unpivot_fields, extra_keys, extra_value, resources=None):
    pass
```

- `unpivot_fields` - List of source field definitions, each definition is an object containing at least these properties:
  - `name` - Either simply the name, or a regular expression matching the name of original field to unpivot.
  - `keys` - A Map between target field name and values for original field
    - Keys should be target field names from `extra_keys`
    - Values may be either simply the constant value to insert, or a regular expression matching the `name`.
- `extra_keys` - List of target field definitions, each definition is an object containing at least these properties (unpivoted column values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `extra_value` - Target field definition - an object containing at least these properties (unpivoted cell values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources


#### filter_rows.py
Filter rows based on inclusive and exclusive value filters.
`filter_rows` accepts equality and inequality conditions and tests each row in the selected resources. If none of the conditions validate, the row will be discarded.

```python
def filter_rows(equals=tuple(), not_equals=tuple(), resources=None):
    pass
```

- `in` - Mapping of keys to values which translate to `row[key] == value` conditions
- `out` - Mapping of keys to values which translate to `row[key] != value` conditions
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources

Both `in` and `out` should be a list of dicts.

### Manipulate package
#### add_metadata.py
Add high-level metadata about your package

```python
def add_metadata(**metadata):
    pass
```

- `metadata` - Any allowed property (according to the [spec]([https://frictionlessdata.io/specs/data-package/#metadata)) can be provided here.

#### concatenate.py
Concatenate multiple streams of data to a single one, resolving differently named columns along the way.

```python
def concatenate(fields, target={}, resources=None):
    pass
```

- `fields` - Mapping of fields between the sources and the target, so that the keys are the _target_ field names, and values are lists of _source_ field names.
  This mapping is used to create the target resources schema.
  Note that the target field name is _always_ assumed to be mapped to itself.
- `target` - Target resource to hold the concatenated data. Should define at least the following properties:
  - `name` - name of the resource
  - `path` - path in the data-package for this file.
  If omitted, the target resource will receive the name `concat` and will be saved at `data/concat.csv` in the datapackage.
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  Resources to concatenate must appear in consecutive order within the data-package.

#### duplicate.py
Duplicate a single stream of data to make two streams

`duplicate` accepts the name of a single resource in the datapackage. 
It will then duplicate it in the output datapackage, with a different name and path.
The duplicated resource will appear immediately after its original.

```python
def duplicate(source=None, target_name=None, target_path=None):
    pass
```

- `source` - The name of the resource to duplicate. 
- `target_name` - Name of the new, duplicated resource.
- `target_path` - Path for the new, duplicated resource.

