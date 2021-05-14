# Builtin Processors

DataFlows comes with a few built-in processors which do most of the heavy lifting in many common scenarios, leaving you to implement only the minimum code that is specific to your specific problem.

### Load and Save Data
- [**load**](#load) - Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)
- [**sources**](#sources) - Combines efficiently data loading from multiple sources
- [**printer**](#printer) - Just prints whatever it sees. Good for debugging.

- [**dump_to_path**](#dump_to_path) - Store the results to a specified path on disk, in a valid datapackage
- [**dump_to_zip**](#dump_to_zip) - Store the results in a valid datapackage, all files archived in one zipped file
- [**dump_to_sql**](#dump_to_sql) - Store the results in a relational database (creates one or more tables or updates existing tables)

### Flow Control
- [**conditional**](#conditional) - Run parts of the flow based on the structure of the datapackage at the calling point
- [**finalizer**](#finalizer) - Call a function when all data had been processed
- [**checkpoint**](#checkpoint) - Cache results of a subflow in a datapackage and load it upon request
- [**parallelize**](#parallelize) - Run a row processor over multiple processes

### Manipulate row-by-row
- [**add_field**](#add_field) - Adds a column to the data
- [**select_fields**](#select_fields) - Selects and keeps some columns in the data
- [**delete_fields**](#delete_fields) - Removes some columns from the data
- [**rename_fields**](#rename_fields) - Changes the names of some columns from the data
- [**add_computed_field**](#add_computed_field) - Adds new fields whose values are based on existing columns
- [**find_replace**](#find_replacepy) - Look for specific patterns in specific fields and replace them with new data
- [**set_type**](#set_typepy) - Modify schema, parse incoming data based on new schema, validate the data in the process
- [**validate**](#validatepy) - Parse incoming data based on existing schema, validate the incoming data in the process

### Manipulate the entire resource
- [**sort_rows**](#sort_rowspy) - Sort incoming data based on key
- [**unpivot**](#unpivotpy) - Unpivot a table - convert one row with multiple value columns to multiple rows with one value column
- [**filter_rows**](#filter_rows) - Filter rows based on inclusive and exclusive value filters
- [**deduplicate**](#deduplicatepy) - Deduplicates rows in resources based on the resources' primary key


### Manipulate package
- [**update_package**](#update_packagepy) - Updates metadata of entire package
- [**update_resource**](#update_resourcepy) - Updates metadata of one or more resources
- [**update_schema**](#update_schemapy) - Update schema properties for one or more resources in the package
- [**set_primary_key**](#set_primary_keypy) - Updates the primary key of one or more resources
- [**concatenate**](#concatenatepy) - Concatenate multiple streams of data to a single one, resolving differently named columns along the way
- [**duplicate**](#duplicatepy) - Duplicate a single stream of data to make two streams

### API Reference

#### load
Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)

```python
def load(source, name=None, resources=None, strip=True, limit_rows=None,
         infer_strategy=None, cast_strategy=None,
         override_schema=None, override_fields=None,
         deduplicate_headers=False,
         on_error=raise_exception,
         **options)
    pass
```

- `source` - location of the data that is to be loaded. This can be either:
    - a local path (e.g. `/path/to/the/data.csv`)
    - a remote URL (e.g. `https://path.to/the/data.csv`)
    - a local path or remote URL to a datapackage.json file (e.g. `https://path.to/data_package/datapackage.json`)
    - a local path or remote URL to a zipped datapackage.json file (e.g. `https://path.to/data_package/datapackage.zip`) - add a `format='datapackage'` option in this scenario
    - a reference to an environment variable containing the source location,
      in the form of `env://ENV_VAR`
    - a tuple containing (datapackage_descriptor, resources_iterator)
    - Other supported links, based on the current support of schemes and formats in [tabulator](https://github.com/frictionlessdata/tabulator-py#schemes)
    - Other formats:
      - XML files
      - Excel-XML format
      - SQL Sources, which on top the current the functionality already supported in `tabulator`, also supports the `query` parameter for using custom query results as a source.

- `resources` - optional, relevant only if source points to a datapackage.json file or datapackage/resource tuple. Value should be one of the following:
    - Name of a single resource to load
    - A regular expression matching resource names to load
    - A list of resource names to load
    - `None` indicates to load all resources
    - The index of the resource in the package
- `options` - based on the loaded file, extra options (e.g. `sheet` for Excel files etc., see the link to tabulator above)

Relevant only when _not_ loading data from a datapackage:
- `strip` - Should string values be stripped from whitespace characters surrounding them.
- `limit_rows` - If provided, will limit the number of rows fetched from the source. Takes an integer value which specifies how many rows of the source to stream.
- `infer_strategy` - Dictates if and how `load` will try to guess the datatypes in the source data:
    - `load.INFER_STRINGS` - All columns will get a `string` datatype
    - `load.INFER_PYTHON_TYPES` - All columns will get a datatype matching their python type
    - `load.INFER_FULL` - All columns will get a datatype matching their python type, except strings which will be attempted to be parsed (e.g `"1" -> 1` etc.) (default)
- `cast_strategy` - Dictates if and how `load` will parse and validate the data against the datatypes in the inferred schema:
    - `load.CAST_TO_STRINGS` - All data will be casted to strings (regardless of how it's stored in the source file) and won't be validated using the schema.
    - `load.CAST_DO_NOTHING` - Data will be passed as-is without modifications or validation
    - `load.CAST_WITH_SCHEMA` - Data will be parsed and casted using the schema and will error in case of faulty data
- `override_schema` - Provided dictionary will be merged into the inferred schema. If `fields` key is set its contents will fully replace the inferred fields array. The same behavior will be applied for all other nested structures.
- `extract_missing_values (bool|dict)` - If `True` it will extract missing values defined in a schema and place in to a new field called `missingValues` with a type `object` in a form of `{field1: value1, field2: value2}`. If a row doesn't have any missing values the field will get an empty object. This option can be a hash with 3 optional keys `source`, `target` and `values` where:
    - `source (str|str[])` - a field or list of fields to extract missing values (default: all fields)
    - `target (str)` - a field to place a missing values mapping (default: `missingValues`)
    - `values (str[])` - an alternative list of missing values (default: `schema['missingValues']`)
- `override_fields` - Provided mapping will patch the inferred `schema.fields` array. In the mapping keys must be field names and values must be dictionaries intended to be merged into the corresponding field's metadata.
- `deduplicate_headers` - (default `False`) If there are duplicate headers and the flag is set to `True` it will rename them using a `header (1), header (2), etc` approach. If there are duplicate headers and the flag is set to `False` it will raise an error.
- `on_error` - Dictates how `load` will behave in case of a validation error.
    Options are identical to `on_error` in `set_type` and `validate`


Some deprecated options:
- `force_strings` - Don't infer data types, assume everything is a string.
    (equivalent to `infer_strategy = INFER_STRINGS, cast_strategy = CAST_TO_STRINGS`)
- `validate` - Attempt to cast data to the inferred data-types.
    (equivalent to `cast_strategy = CAST_WITH_SCHEMA, on_error = raise_exception`)


#### sources
Combines efficiently data loading from multiple sources

```python
def sources(*data_sources):
    pass
```

- `data_sources` - a list of iterables, `load` calls, or generators. These will be efficiently combined into multiple resources in the output stream.

#### printer
Just prints whatever it sees. Good for debugging.

```python
def printer(num_rows=10, last_rows=None, fields=None, resources=None,
            header_print=_header_print, table_print=_table_print,
            max_cell_size=100, **tabulate_kwargs):
    pass
```

- `num_rows` - modify the number of rows to preview, printer will print multiple samples of this number of rows from different places in the stream
- `last_rows` - optional, defaults to the value of `num_rows`
- `fields` - optional, list of field names to preview
- `resources` - optional, allows to limit the printed resources, same semantics as `load` processor `resources` argument
- `header_print` - optional, callable used to print each resource header
- `table_print` - optional, callable used to print the table data
- `max_cell_size` - optional, limit the maximum cell size to the given number of chars
- `**tabulate_kwargs` - additional kwargs passed to [tabulate](https://bitbucket.org/astanin/python-tabulate), allows to customize the printed tables

If you are running from a [Jupyter](https://jupyter.org/) notebook, add `tablefmt='html'` to render an html table:

```
printer(tablefmt='html')
```

#### dump_to_path
Store the results to a specified path on disk, in a valid datapackage

```python
def dump_to_path(out_path='.',
                 force_format=True, format='csv',
                 temporal_format_property='format',
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
- `format` - Specifies the type of output files to be generated (if `force-format` is true): `csv` (the default), `json` or `geojson`
- `temporal-format-property` - Specifies a property to be used for temporal values serialization. For example, if some field has a property `outputFormat: %d/%m/%y` setting `temporal-format-property` to `outputFormat` will lead to using this format for this field serialization.
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

- `use_titles` - If set to True, will use the field titles for header rows rather then field names (relevant for csv format only, default `False`)

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

```python
def dump_to_sql(tables,
                engine='env://DATAFLOWS_DB_ENGINE',
                updated_column=None, updated_id_column=None,
                counters={}, batch_size=1000):
    pass
```

- `tables` - Mapping between resources and DB tables. Keys are table names, values are objects with the following attributes:
  - `resource-name` - name of the resource that should be dumped to the table
  - `mode` - How data should be written to the DB.
    Possible values:
      - `rewrite` (the default) - rewrite the table, all previous data (if any) will be deleted.
      - `append` - write new rows without changing already existing data.
      - `update` - update the table based on a set of "update keys".
        For each new row, see if there already an existing row in the DB which can be updated (that is, an existing row
        with the same values in all of the update keys).
        If so - update the rest of the columns in the existing row. Otherwise - insert a new row to the DB.
  - `update_keys` - Only applicable for the `update` mode. A list of field names that should be used to check for row existence.
        If left unspecified, will use the schema's `primaryKey` as default.
  - `indexes` - TBD
- `engine` - Connection string for connecting to the SQL Database (URL syntax)
  Also supports `env://<environment-variable>`, which indicates that the connection string should be fetched from the indicated environment variable.
  If not specified, assumes a default of `env://DATAFLOWS_DB_ENGINE`
- `updated_column` - Optional name of a column that will be added to the output data with boolean value
  - `true` - row was updated
  - `false` - row was inserted
- `updated_id_column` - Optional name of a column that will be added to the output data containing the id of the updated row in DB.
- `batch_size` - Maximum amount of rows to write at the same time to the DB (default 1000)
- `use_bloom_filter` - Preprocess existing DB data to improve update performance (default: True)

### Flow Control

#### conditional

Run parts of the flow based on the structure of the datapackage at this point.

```python
def conditional(predicate, flow):
    pass
```

- `predicate` - a boolean function, receiving a single parameter which is a `Package.datapacakge` and returns true/false
- `flow` - a `Flow` to chain to the processing pipeline if the predicate is positive.

Example - add a field if it doesn't exist in the first resource in the data package:

```python
def no_such_field(field_name):
    def func(dp):
        return all(field_name != f.name for f in dp.resources[0].schema.fields)
    return func

Flow(
    # ...
    conditional(
        no_such_field('my-field', Flow(
            add_field('my-field', 'string', 'default-value')
        ))
    )
    # ...
)
```


#### finalizer

Call a function when all data had been processed at the calling point.

```python
def finalizer(callback):
    pass
```

- `callback` - a callback function which the processor will call once all data finished passing through it.
               In case `callback` has a `stats` parameter, it will also receive the stats collected up till this point.

Example - show a message when done loading a file

```python
def print_done():
    print('done loading')

Flow(
    # ...
    load(...),
    finalizer(print_done)
    # ...
)
```

#### checkpoint

Save results from running a series of steps, if checkpoint exists - loads from checkpoint instead of running the steps.

Checkpoint invalidation should be handled manually - by deleting the checkpoint path (by default at `.checkpoints/<checkpoint_name>`)

```python
def checkpoint(checkpoint_name, checkpoint_path='.checkpoints', steps=None, resources=None):
    pass
```

- `checkpoint_name` - The checkpoint is saved to a datapackage at `<checkpoint_path>/<checkpoint_name>`
- `checkpoint_path` - Relative or absolute path to save checkpoints at, default to relative path `.checkpoints`
- `steps` - Iterable of steps to checkpoint, when not provided, uses the previous steps in the flow
- `resources` - Limit the checkpointing only to specific resources, same semantics as `load` processor `resources` argument

To improve code readability, the checkpoint processor can be placed anywhere in the flow and it will checkpoint the previous steps:

```python
from dataflows import Flow, checkpoint, printer

f = Flow(
    load('http://example.com/large_resource.csv'),
    load('http://example.com/another_large_resource.csv'),
    checkpoint('load_data'),
    process(),
    checkpoint('process_data'),
    printer()
)

# first run will load the resources and save the checkpoints
f.process()

# next run will load the resources from the last checkpoint
f.process()
```

#### parallelize

Run a row processor over multiple processes, making to make better use of multiple cores and compensate for long i/o waits.

```python
def parallelize(row_func, num_processors=None, predicate=None, resources=None):
    pass
```

- `row_func` - A function handling a single row in a resource, to be run in parallel in multiple processes
- `num_processors` - Number of processors to use. If not specified, will make an educated guess based on the current machine's architecture.
- `predicate` - A function which accepts a row and returns a boolean. If provided, only rows for which `predicate(row) is True` will be processed, others will be passed through unmodified.
- `resources` - Only apply the function on specific resources, same semantics as `load` processor `resources` argument

A few important notes regarding the `parallelize` processor - 
- `row_func` runs in the context of a new process, so don't assume it has access to any global variables or state that were available in the main process
- Due to its parallel nature, rows might change their order in the output of this processor.
- `predicate` is an important tool in optimizing perforance - since it's more tiem consuming, we only want to pass rows to the worker processes if there's work to be done there. In fact, until the predicate returns `True` for the first time, no worker processes are even created.

Example:

```python
import requests 

data = [dict(url=url) for url in list_of_urls]

def fetch(row):
  row['data'] = requests.get(row['url']).content

Flow(
  data,
  parallelize(fetch),  # Will fetch all data in parallel
  dump_to_path('scraper')
).process()
```


### Manipulate row-by-row
#### add_field
Adds a new field (column) to the streamed resources

`add_field` accepts a list of resources and a fields to add

```python
def add_field(name, type, default=None, resources=None, **options):
    pass
```

- `name` - Name of field to add
- `type` - Type of field to add
- `default` - Value to assign to the field
    - can be a literal value to be added to all rows
    - can be a callable which gets a row and returns a value for the newly added field
- `options` - Other properties of the newly added field
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

#### select_fields
Select fields (columns) in streamed resources and remove all other fields. Can also be used to reorder schema fields.

`select_fields` accepts a list of resources and list of fields to select, either as literal strings or as regular expressions

_Note: if multiple resources provided, all of them should contain all fields to select_

```python
def select_fields(fields, resources=None, regex=True):
    pass
```

- `fields` - List of field (column) names to keep (supports regular expressions as well - if `regex` is `True`)
- `regex` - allow specifying regular expressions as field names (defaults to `True`)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

#### delete_fields
Delete fields (columns) from streamed resources

`delete_fields` accepts a list of resources and list of fields to remove

_Note: if multiple resources provided, all of them should contain all fields to delete_

```python
def delete_fields(fields, resources=None, regex=True):
    pass
```

- `fields` - List of field (column) names to be removed (exact names or regular expressions for matching field names)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `regex` - if set to `False` field names will be interpreted as strings not as regular expressions (`True` by default)

#### rename_fields
Changes the names of some fields (columns) from streamed resources

`rename_fields` accepts a list of resources and list of fields to rename

```python
def delete_fields(fields, resources=None, regex=True):
    pass
```

- `fields` - Dictionary mapping original field names to renamed field names (exact names or regular expressions for matching field names)
  Examples:
    - ```dict(a='A', b='B')   # renaming a->A, b->B```
    - ```{r'a(\d)': r'A\1'}   # renaming a1->A1, a2->A2, ...```
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `regex` - if set to `False` field names will be interpreted as strings not as regular expressions (`True` by default)

#### add_computed_field
Adds new fields whose values are based on existing columns.

`add_computed_field` accepts one or more fields to add to existing resources. It will modify the schemas and add the new fields to processed resources. The content of the new fields can be a constant, based on other columns with some predefined operations or be dynamically calculated using a user-specified function.

```python
def add_computed_field(*args, **kw, resources=None):
    pass
```

This function accepts a single field specification as keyword parameters, or a list of field specifications as a list of dicts in the first positional argument.

A new field specification has the following keys:
  - `target` - can be the name of the new field, or a full field specification (dict with `name`, `type` and other fields)
  - `operation`: operation to perform on values of pre-defined columns of the same row.
    Can be a function that accepts a row and returns the value of the new field, or a string containing the name of a predefined operation.
    Available operations:
    - `constant` - add a constant value
    - `sum` - summed value for given columns in a row.
    - `avg` - average value from given columns in a row.
    - `min` - minimum value among given columns in a row.
    - `max` - maximum value among given columns in a row.
    - `multiply` - product of given columns in a row.
    - `join` - joins two or more column values in a row.
    - `format` - Python format string used to form the value Eg:  `my name is {first_name}`.
  - `source` - list of columns the operations should be performed on (Not required in case of `format` and `constant`).
  - `with` (also accepts `with_`) - String passed to `constant`, `format` or `join` operations
    - in `constant` - used as constant value
    - in `format` - used as Python format string with existing column values Eg: `{first_name} {last_name}`
    - in `join` - used as delimiter

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

Examples:
```python
Flow(
    # ... adding single fields with built-in operations
    add_computed_field(target='the-avg', operation='avg', source=['col-a', 'col-b']),
    add_computed_field(target='the-sum', operation='sum', source=['col-a', 'col-b']),
    # ... adding two fields in a single operation
    add_computed_field([
        dict(target='formatted', operation='format', with_='{col-a}-{col-b}'),
        dict(target=dict(name='created', type='date'), operation='constant', with_=datetime.today()),
    ]),
    # ... and with a custom function
    add_computed_field(target=dict(name='power', type='integer'),
                       operation=lambda row: row['col-a'] ** row['col-b'],
                       resources='my-resource-name'),
)
```

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
  - The index of the resource in the package

#### set_type.py
Sets a field's data type and type options and validates its data based on its new type definition.

By default, this processor modifies the last resource in the package.

```python
def set_type(name, resources=-1, regex=True, on_error=None, transform=None, **options):
    pass
```

- `name` - the name of the field to modify (or a regular expression to match multiple fields)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `regex` - if set to `False` field names will be interpreted as strings not as regular expressions (`True` by default)
- `transform` - callable to be used to transform the value while setting its type.

  This function should have the signature `func(value, field_name=None, row=None)` and return the transformed value.

- `on_error` - callback function to be called when a validation error occurs.

  Function can have the signature
  - `callback(resource_name, row, row_index, exception)` - will receive the resource name, the contents of the offending row, its index and the raised exception
  - `callback(resource_name, row, row_index, exception, field)` - will receive the resource name, the contents of the offending row, its index and the raised exception and the offending field (as a Field object)

  This function may raise an exception, return `True` for keeping the row anyway or `False` for dropping it.

  A few predefined options are:
  - `dataflows.base.schema_validator.raise_exception` - the default behaviour, will raise a `dataflows.ValidationError` exception.
  - `dataflows.base.schema_validator.drop` - drop invalid rows
  - `dataflows.base.schema_validator.ignore` - ignore all errors
  - `dataflows.base.schema_validator.clear` - clear invalid fields to None
- `options` - options to set for the field. Most common ones would be:
  - `type` - set the data type (e.g. `string`, `integer`, `number` etc.)
  - `format` - e.g. for date fields
  etc.
 (more info on possible options can be found in the [tableschema spec](https://frictionlessdata.io/specs/table-schema/))


#### validate.py
Validate incoming data based on existing type definitions or custom

```python
def validate(*args, resources=None, on_error=None):
    pass
```

- `args` - validation parameters, could be one of the above
  - No arguments - will validate the data based on the json table schema specification
  - One argument - will be treated as a row validation function, returns `True` when row is valid
  - Two arguments - will be treated as a `(field_name, validator)` tuple, validator being a function receiving the value of `field_name` in each row and returns `True` if the value is valid.
- `resources` - which resources to validate
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `on_error` - callback function to be called when a validation error occurs.
  See `set_type` for details.

### Manipulate the entire resource
#### sort_rows.py
Sort incoming data based on key.

`sort_rows` accepts a list of resources and a key (as a Python format string on row fields).
It will output the rows for each resource, sorted according to the key (in ascending order by default).


```python
def sort_rows(key, resources=None, reverse=False):
    pass
```

- `key` - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `reverse` - Set to True to return results in descending order

#### unpivot.py
Unpivot a table - convert one row with multiple value columns to multiple rows with one value column

```python
def unpivot(unpivot_fields, extra_keys, extra_value, regex=True, resources=None):
    pass
```

- `unpivot_fields` - List of source field definitions, each definition is an object containing at least these properties:
  - `name` - Either simply the name, or a regular expression matching the name of original field to unpivot (if `regex` is `True`)
  - `keys` - A Map between target field name and values for original field
    - Keys should be target field names from `extra_keys`
    - Values may be either simply the constant value to insert, or a regular expression matching the `name`.
- `extra_keys` - List of target field definitions, each definition is an object containing at least these properties (unpivoted column values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `extra_value` - Target field definition - an object containing at least these properties (unpivoted cell values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `regex` - Should regular expressions functionality be enabled (see `name`)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

Examples:
```
2000,2001,2002
a1,b1,c1,d1
a2,b2,c2,d2
```

Let's unpivot the above table so that it has normalized form:
```python
from dataflows import Flow, unpivot

data = [
    {'2000': 'a1', '2001': 'b1', '2002': 'c1'},
    {'2000': 'a2', '2001': 'b2', '2002': 'c2'},
    {'2000': 'a3', '2001': 'b3', '2002': 'c3'}
]

# Using regex, we can select all headers that can be a year:
unpivoting_fields = [
    { 'name': '([0-9]{4})', 'keys': {'year': r'\1'} }
]

# A newly created column header would be 'year' with type 'year':
extra_keys = [ {'name': 'year', 'type': 'year'} ]
# And values will be placed in the 'value' column with type 'string':
extra_value = {'name': 'value', 'type': 'string'}


Flow(data, unpivot(unpivoting_fields, extra_keys, extra_value)).results()[0]
# The last statement would print unpivoted data into stdout:
# [[ {'year': 2000, 'value': 'a1'}, {'year': 2001, 'value': 'b1'}, ... ]]
```

As a result, I have a normalized data table:

```
year,value
2000,a1
2000,a2
2000,a3
2001,b1
2001,b2
2001,b3
2002,c1
2002,c2
2002,c3
```

#### filter_rows
Filter rows based on inclusive and exclusive value filters.
`filter_rows` accepts equality and inequality conditions and tests each row in the selected resources. If none of the conditions validate, the row will be discarded.

```python
def filter_rows(condition=None, equals=tuple(), not_equals=tuple(), resources=None):
    pass
```

- `condition` - Callable, receiving a row and returning True/False (if True then will pass row, otherwise will drop it)

  If `condition` is not provided:
  - `equals` - Mapping of keys to values which translate to `row[key] == value` conditions
  - `not_equals` - Mapping of keys to values which translate to `row[key] != value` conditions

  Both `equals` and `not_equals` should be a list of dicts.

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

#### deduplicate.py
Deduplicates rows in resources based on the resources' primary key

`deduplicate` accepts a resource specifier.
For each resource, it will output only unique rows (based on the values in the primary key fields). Rows with duplicate primary keys will be ignored.

```python
def deduplicate(resources=None):
    pass
```

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

### Manipulate package
#### update_package.py
Add high-level metadata about your package

```python
def update_package(**metadata):
    pass
```

- `metadata` - Any allowed property (according to the [spec]([https://frictionlessdata.io/specs/data-package/#metadata)) can be provided here.

(`add_metadata` is an alias for `update_package` kept for backward compatibility)

#### update_resource.py
Update metadata for one or more resources in the package

```python
def update_resource(resources, **metadata):
    pass
```

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `metadata` - Any allowed property (according to the [spec]([https://frictionlessdata.io/specs/data-resource/#metadata)) can be provided here.

You can use `update_resource` to rename a resource like so:
```python
  update_resource('current-name', name='new-name')
```

#### update_schema.py
Update schema properties for one or more resources in the package

```python
def update_schema(resources, **metadata):
    pass
```

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `metadata` - Any allowed schema property (according to the [spec]([https://frictionlessdata.io/specs/table-schema/#descriptor)) can be provided here.

You can use `update_schema` to add a `missingValues` property, change the primary key etc.

#### set_primary_key.py
Updates the primary key for one or more resources in the package

```python
def set_primary_key(primary_key, resources=None):
    pass
```

- `primary_key` - List of field names to be set as the resource's primary key
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package


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
  - The index of the resource in the package
  Resources to concatenate must appear in consecutive order within the data-package.

#### duplicate.py
Duplicate a single stream of data to make two streams

`duplicate` accepts the name of a single resource in the datapackage.
It will then duplicate it in the output datapackage, with a different name and path.
The duplicated resource will appear immediately after its original.

```python
def duplicate(source=None, target_name=None, target_path=None, duplicate_to_end=False):
    pass
```

- `source` - The name of the resource to duplicate.
- `target_name` - Name of the new, duplicated resource.
- `target_path` - Path for the new, duplicated resource.
- `duplicate_to_end` - Add the duplicate to the end of the resource list.

#### join.py
Joins two streamed resources.

"Joining" in our case means taking the *target* resource, and adding fields to each of its rows by looking up data in the _source_ resource.

A special case for the join operation is when there is no target stream, and all unique rows from the source are used to create it.
This mode is called _deduplication_ mode - The target resource will be created and de-duplicated rows from the source will be added to it.

```python
def join(source_name, source_key, target_name, target_key, fields={}, mode='half-outer', source_delete=True):
    pass

def join_with_self(resource_name, join_key, fields):
    pass
```

- `source_name` - name of the _source_ resource
- `source_key` - One of
    - List of field names which should be used as the lookup key
    - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`). It's possible to use `#` as a special field name to include a row number (startring from the first row after the headers row) e.g. `{#}:{field_name_2}`.
- `source_delete` - delete source from data-package after joining (`True` by default)

- `target_name` - name of the _target_ resource to hold the joined data.
- `target_key`, `join_key` - as in `source_key`

- `fields` - mapping of fields from the source resource to the target resource.
  Keys should be field names in the target resource.
  You can use the special catchall key `*` which will apply for all fields in the source which were not specifically mentioned.

  Values can define two attributes:
  - `name` - field name in the source (by default is the same as the target field name)

  - `aggregate` - aggregation strategy (how to handle multiple _source_ rows with the same key). Can take the following options:
    - `sum` - summarise aggregated values.
      For numeric values it's the arithmetic sum, for strings the concatenation of strings and for other types will error.

    - `avg` - calculate the average of aggregated values.

      For numeric values it's the arithmetic average and for other types will err.

    - `max` - calculate the maximum of aggregated values.

      For numeric values it's the arithmetic maximum, for strings the dictionary maximum and for other types will error.

    - `min` - calculate the minimum of aggregated values.

      For numeric values it's the arithmetic minimum, for strings the dictionary minimum and for other types will error.

    - `first` - take the first value encountered

    - `last` - take the last value encountered

    - `count` - count the number of occurrences of a specific key
      For this method, specifying `name` is not required. In case it is specified, `count` will count the number of non-null values for that source field.

    - `counters` - count the number of occurrences of distinct values
      Will return an array of 2-tuples of the form `[value, count-of-value]`.

    - `set` - collect all distinct values of the aggregated field, unordered

    - `array` - collect all values of the aggregated field, in order of appearance

    - `any` - pick any value.

    By default, `aggregate` takes the `any` value.

  If neither `name` or `aggregate` need to be specified, the mapping can map to the empty object `{}` or to `null`.
- `mode` - Enum,
  - if `inner`, failed lookups in the source will result in dropping the row from the target. It uses the same principle as the SQL's inner join.
  - If `half-outer` (the default), failed lookups in the source will result in "null" values at the source. It uses the same principle as the SQL's left/right outer join.
  - if `full-outer`, failed lookups in the source will result in "null" values at the source. All not used values from the target will result in "null" values at the source. It uses the same principle as the SQL's full outer join.
- `full` - Boolean [DEPRECATED - use `mode`],
  - If `True` (the default), failed lookups in the source will result in "null" values at the source.
  - if `False`, failed lookups in the source will result in dropping the row from the target.

_Important: the "source" resource **must** appear before the "target" resource in the data-package._

**Examples:**

With these two sources:

`characters`:
first_name  |  house     |last_name         |age
------------|----------|-----------|----------
Jaime       |Lannister |Lannister          |34
Tyrion      |Lannister |Lannister          |27
Cersei      |Lannister |Lannister          |34
Jon         |Stark     |Snow               |17
Sansa       |Stark     |Stark              |14
Rickon      |Stark     |Stark               |5
Arya        |Stark     |Stark              |11
Bran        |Stark     |Stark              |10
Daenerys    |Targaryen |Targaryen          |16

`houses`:
|house
|------------------
|House of Lannister
|House of Greyjoy
|House of Stark
|House of Targaryen
|House of Martell
|House of Tyrell

*Joining two resources*:
```python
Flow(#...
    join(
        'characters',
        'House of {house}', # Note we need to format the join keys so they match
        'houses',
        '{house}',
        dict(
            max_age={
                'name': 'age',
                'aggregate': 'max'
            },
            avg_age={
                'name': 'age',
                'aggregate': 'avg'
            },
            representative={
                'name': 'first_name',
                'aggregate': 'last'
            },
            representative_age={
                'name': 'age'
            },
            number_of_characters={
                'aggregate': 'count'
            },
            last_names={
                'name': 'last_name',
                'aggregate': 'counters'
            }
        ),
        False, # Don't do a full join (i.e. discard houses which have no characters)
        True   # Remove the source=characters resource from the output
    )
)
```

Output:
house               | avg_age  | last_names                | max_age  | number_of_characters | representative | representative_age
--------------------|----------|---------------------------|----------|----------------------|----------------|--------------------
House of Lannister  |  31.6667 | [('Lannister', 3)]        |    34    |         3            | Cersei         |        34
House of Stark      |   11.4   | [('Stark', 4), ('Snow', 1)] |   17   |         5            | Bran           |        10
House of Targaryen  |   16     | [('Targaryen', 1)]        |    16    |         1            | Daenerys       |        16

*Self-Joining a resource (find the youngest member of each house)*:
```python
Flow(#...
    sort_rows('{age:02}'),
    join_with_self(
        'characters',
        '{house}',
        {
            'the_house': {
                'name': 'house'
            },
            '*': {
                'aggregate': 'first'
            },
        }
    ),
)
```

Output:
age|first_name  |last_name  |the_house
----------|------------|-----------|-----------
27|Tyrion      |Lannister  |Lannister
5|Rickon      |Stark      |Stark
16|Daenerys    |Targaryen  |Targaryen

*Joining using row numbers*:
`source`:
| values |
|--------|
| value1 |
| value2 |

`target`:
| id | names |
|----|-------|
| 01 | name1 |
| 02 | name2 |

```python
Flow(#...
    join(
        source_name='source',
        source_key=['#'],
        target_name='target',
        target_key=['#'],
        fields={'values': {'name': 'values'}}
    ),
)
```

Output:
| id | names | values |
|----|-------|--------|
| 01 | name1 | value1 |
| 02 | name2 | value2 |
