# Builtin Processors

DataFlows comes with a few built-in processors which do most of the heavy lifting in many common scenarios, leaving you to implement only the minimum code that is specific to your specific problem.

### Load and Save Data
- **load** - Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)
- **printer** - Just prints whatever it sees. Good for debugging.

- **dump_to_path** - Store the results to a specified path on disk, in a valid datapackage
- **dump_to_zip** - Store the results in a valid datapackage, all files archived in one zipped file
- **dump_to_sql** - Store the results in a relational database (creates one or more tables or updates existing tables)

- **checkpoint** - Cache results of a subflow in a datapackage and load it upon request

### Manipulate row-by-row
- **add_field** - Adds a column to the data
- **select_fields** - Selects and keeps some columns in the data
- **delete_fields** - Removes some columns from the data
- **add_computed_field** - Adds new fields whose values are based on existing columns
- **find_replace** - Look for specific patterns in specific fields and replace them with new data
- **set_type** - Modify schema, parse incoming data based on new schema, validate the data in the process
- **validate** - Parse incoming data based on existing schema, validate the incoming data in the process

### Manipulate the entire resource
- **sort_rows** - Sort incoming data based on key
- **unpivot** - Unpivot a table - convert one row with multiple value columns to multiple rows with one value column
- **filter_rows** - Filter rows based on inclusive and exclusive value filters

### Manipulate package
- **update_package** - Updates metadata of entire package
- **update_resource** - Updates metadata of one or more resources
- **set_primary_key** - Updates the primary key of one or more resources
- **concatenate** - Concatenate multiple streams of data to a single one, resolving differently named columns along the way
- **duplicate** - Duplicate a single stream of data to make two streams

### API Reference

#### load
Loads data from various source types (local files, remote URLS, Google Spreadsheets, databases...)

```python
def load(source, name=None, resources=None, validate=False, strip=True, force_strings=False, **options):
    pass
```

- `source` - location of the data that is to be loaded. This can be either:
    - a local path (e.g. `/path/to/the/data.csv`)
    - a remote URL (e.g. `https://path.to/the/data.csv`)
    - Other supported links, based on the current support of schemes and formats in [tabulator](https://github.com/frictionlessdata/tabulator-py#schemes)
    - a local path or remote URL to a datapackage.json file (e.g. `https://path.to/data_package/datapackage.json`)
    - a reference to an environment variable containing the source location, 
      in the form of `env://ENV_VAR`
    - a tuple containing (datapackage_descriptor, resources_iterator)
- `resources` - optional, relevant only if source points to a datapackage.json file or datapackage/resource tuple. Value should be one of the following:
    - Name of a single resource to load
    - A regular expression matching resource names to load
    - A list of resource names to load
    - `None` indicates to load all resources
    - The index of the resource in the package
- `options` - based on the loaded file, extra options (e.g. `sheet` for Excel files etc., see the link to tabulator above)

Relevant only when _not_ loading data from a datapackage:
- `force_strings` - Don't infer data types, assume everything is a string.
- `validate` - Attempt to cast data to the inferred data-types.
- `strip` - Should string values be stripped from whitespace characters surrounding them. 

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
                counters={}):
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
- `default` - Default value to assign to the field
- `options` - Other properties of the newly added field
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

#### select_fields
Select fields (columns) in streamed resources and remove all other fields. Can also be used to reorder schema fields.

`select_fields` accepts a list of resources and list of fields to remove

_Note: if multiple resources provided, all of them should contain all fields to select_

```python
def select_fields(fields, resources=None):
    pass
```

- `fields` - List of field (column) names to keep
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
def delete_fields(fields, resources=None):
    pass
```

- `fields` - List of field (column) names to be removed
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

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
  - The index of the resource in the package


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
def set_type(name, resources=-1, **options):
    pass
```

- `name` - the name of the field to modify (or a regular expression to match multiple fields)
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package
- `options` - options to set for the field. Most common ones would be:
  - `type` - set the data type (e.g. `string`, `integer`, `number` etc.)
  - `format` - e.g. for date fields 
  etc.
 (more info on possible options can be found in the [tableschema spec](https://frictionlessdata.io/specs/table-schema/))

#### validate.py
Validate incoming data based on existing type definitions.

```python
def validate():
    pass
```

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
  - The index of the resource in the package


#### filter_rows
Filter rows based on inclusive and exclusive value filters.
`filter_rows` accepts equality and inequality conditions and tests each row in the selected resources. If none of the conditions validate, the row will be discarded.

```python
def filter_rows(equals=tuple(), not_equals=tuple(), resources=None):
    pass
```

- `equals` - Mapping of keys to values which translate to `row[key] == value` conditions
- `not_equals` - Mapping of keys to values which translate to `row[key] != value` conditions
- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - `None` indicates operation should be done on all resources
  - The index of the resource in the package

Both `equals` and `not_equals` should be a list of dicts.

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
def duplicate(source=None, target_name=None, target_path=None):
    pass
```

- `source` - The name of the resource to duplicate. 
- `target_name` - Name of the new, duplicated resource.
- `target_path` - Path for the new, duplicated resource.

#### join.py
Joins two streamed resources.

"Joining" in our case means taking the *target* resource, and adding fields to each of its rows by looking up data in the _source_ resource.

A special case for the join operation is when there is no target stream, and all unique rows from the source are used to create it.
This mode is called _deduplication_ mode - The target resource will be created and de-duplicated rows from the source will be added to it.

```python
def join(source_name, source_key, target_name, target_key, fields={}, full=True, source_delete=True):
    pass

def join_self(source_name, source_key, target_name, fields):
    pass
```

- `source_name` - name of the _source_ resource
- `source_key` - One of
    - List of field names which should be used as the lookup key
    - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`)
- `source_delete` - delete source from data-package after joining (`True` by default)

- `target_name` - name of the _target_ resource to hold the joined data. 
- `target_key` - as in `source_key`

- `fields` - mapping of fields from the source resource to the target resource.
  Keys should be field names in the target resource.
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
- `full` - Boolean,
  - If `True` (the default), failed lookups in the source will result in "null" values at the source.
  - if `False`, failed lookups in the source will result in dropping the row from the target.

_Important: the "source" resource **must** appear before the "target" resource in the data-package._
