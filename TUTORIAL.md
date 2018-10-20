
# Dataflows Tutorial

This tutorial is built as a Jupyter notebook which allows you to run and modify the code inline and can be used as a starting point for new Dataflows projects.

To get started quickly without any installation, click here: [![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/datahq/dataflows/master?filepath=TUTORIAL.ipynb)

If you want, you can just skip the installation section and follow the tutorial as-is, copy-pasting the relevant code example to your Python interpreter.


## Installation

Easiest way to get started on any OS is to [Download and install the latest Python 3.7 Miniconda distribution](https://conda.io/miniconda.html)

Open a terminal with Miniconda (or Anaconda) and run the following to create a environment:

```sh
conda create -n dataflows 'python>=3.7' jupyter jupyterlab ipython leveldb
```

Activate the environment and install dataflows:

```sh
. activate dataflows
pip install -U dataflows[speedup]
```

The above command installs Dataflows optimized for speed, if you encounter problems installing it, install without the `[speedup]` suffix.

Save the tutorial notebook in current working directory (right-click and save on following link): https://raw.githubusercontent.com/datahq/dataflows/master/TUTORIAL.ipynb

Start Jupyter Lab:

```sh
jupyter lab
```

Double-click the tutorial notebook you downloaded from the sidebar of Jupyter Lab

## Learn how to write your own processing flows

Let's start with the traditional 'hello, world' example:


```python
from dataflows import Flow

data = [
  {'data': 'Hello'},
  {'data': 'World'}
]

def lowerData(row):
    row['data'] = row['data'].lower()

Flow(
      data,
      lowerData
).results()[0]
```




    [[{'data': 'hello'}, {'data': 'world'}]]



This very simple flow takes a list of `dict`s and applies a row processing function on each one of them.

We can load data from a file instead:


```python
%%writefile beatles.csv
name,instrument 
john,guitar
paul,bass
george,guitar
ringo,drums
```

    Writing beatles.csv



```python
from dataflows import Flow, load

def titleName(row):
    row['name'] = row['name'].title()

Flow(
      load('beatles.csv'),
      titleName
).results()[0]
```




    [[{'name': 'John', 'instrument': 'guitar'},
      {'name': 'Paul', 'instrument': 'bass'},
      {'name': 'George', 'instrument': 'guitar'},
      {'name': 'Ringo', 'instrument': 'drums'}]]



The source file can be a CSV file, an Excel file or a Json file. You can use a local file name or a URL for a file hosted somewhere on the web.

Data sources can be generators and not just lists or files. Let's take as an example a very simple scraper:


```python
from dataflows import Flow, printer

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
            for row in table.find('tbody').findall('tr'):
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

Flow(
      country_population(),
      printer(num_rows=1, tablefmt='html')
).process()[1]
```


<h3>res_1</h3>



<table>
<thead>
<tr><th>#  </th><th>name
(string)                 </th><th>population
(string)              </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>China           </td><td>1,394,720,000</td></tr>
<tr><td>2  </td><td>India           </td><td>1,338,480,000</td></tr>
<tr><td>...</td><td>                </td><td>             </td></tr>
<tr><td>240</td><td>Pitcairn Islands</td><td>50           </td></tr>
</tbody>
</table>





    {}



This is nice, but we do prefer the numbers to be actual numbers and not strings.

In order to do that, let's simply define their type to be numeric and truncate to millions:


```python
from dataflows import Flow, set_type

Flow(
    country_population(),
    set_type('population', type='number', groupChar=','),
    lambda row: dict(row, population=row['population']/1000000),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```


<h3>res_1</h3>



<table>
<thead>
<tr><th>#  </th><th>name
(string)                 </th><th style="text-align: right;">         population
(number)</th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>China           </td><td style="text-align: right;">1394.72 </td></tr>
<tr><td>2  </td><td>India           </td><td style="text-align: right;">1338.48 </td></tr>
<tr><td>...</td><td>                </td><td style="text-align: right;">        </td></tr>
<tr><td>240</td><td>Pitcairn Islands</td><td style="text-align: right;">   5e-05</td></tr>
</tbody>
</table>





    {}



Data is automatically converted to the correct native Python type.

Apart from data-types, it's also possible to set other constraints to the data. If the data fails validation (or does not fit the assigned data-type) an exception will be thrown - making this method highly effective for validating data and ensuring data quality. 

What about large data files? In the above examples, the results are loaded into memory, which is not always preferrable or acceptable. In many cases, we'd like to store the results directly onto a hard drive - without having the machine's RAM limit in any way the amount of data we can process.

We do it by using _dump_ processors:


```python
from dataflows import Flow, set_type, dump_to_path

Flow(
    country_population(),
    set_type('population', type='number', groupChar=','),
    dump_to_path('country_population')
).process()[1]
```




    {'count_of_rows': 240,
     'bytes': 5277,
     'hash': 'b293685b58a33bd7b02cc275d19d3a95',
     'dataset_name': None}



Running this code will create a local directory called `county_population`, containing two files:


```python
import glob
print("\n".join(glob.glob('country_population/*')))
```

    country_population/res_1.csv
    country_population/datapackage.json


The CSV file - `res_1.csv` - is where the data is stored. The `datapackage.json` file is a metadata file, holding information about the data, including its schema.

We can now open the CSV file with any spreadsheet program or code library supporting the CSV format - or using one of the **data package** libraries out there, like so:


```python
from datapackage import Package
pkg = Package('country_population/datapackage.json')
it = pkg.resources[0].iter(keyed=True)
print(next(it))
```

    {'name': 'China', 'population': Decimal('1394720000')}


Note how using the data package meta-data, data-types are restored and there's no need to 're-parse' the data. This also works with other types too, such as dates, booleans and even `list`s and `dict`s.

So far we've seen how to load data, process it row by row, and then inspect the results or store them in a data package.

Let's see how we can do more complex processing by manipulating the entire data stream:


```python
from dataflows import Flow, set_type, dump_to_path, printer

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

Flow(
    all_triplets(),
    set_type('a', type='integer'),
    set_type('b', type='integer'),
    set_type('c', type='integer'),
    filter_pythagorean_triplets,
    dump_to_path('pythagorean_triplets'),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```


<h3>res_1</h3>



<table>
<thead>
<tr><th>#  </th><th style="text-align: right;">   a
(integer)</th><th style="text-align: right;">   b
(integer)</th><th style="text-align: right;">   c
(integer)</th></tr>
</thead>
<tbody>
<tr><td>1  </td><td style="text-align: right;"> 3</td><td style="text-align: right;"> 4</td><td style="text-align: right;"> 5</td></tr>
<tr><td>2  </td><td style="text-align: right;"> 5</td><td style="text-align: right;">12</td><td style="text-align: right;">13</td></tr>
<tr><td>...</td><td style="text-align: right;">  </td><td style="text-align: right;">  </td><td style="text-align: right;">  </td></tr>
<tr><td>6  </td><td style="text-align: right;">12</td><td style="text-align: right;">16</td><td style="text-align: right;">20</td></tr>
</tbody>
</table>





    {'count_of_rows': 6,
     'bytes': 744,
     'hash': '1f0df7ed401ccff9f6c1674e98c62467',
     'dataset_name': None}



The `filter_pythagorean_triplets` function takes an iterator of rows, and yields only the ones that pass its condition. 

The flow framework knows whether a function is meant to hande a single row or a row iterator based on its parameters: 

- if it accepts a single `row` parameter, then it's a row processor.
- if it accepts a single `rows` parameter, then it's a rows processor.
- if it accepts a single `package` parameter, then it's a package processor.

Let's see a few examples of what we can do with a package processors.

First, let's add a field to the data:


```python
from dataflows import Flow, load, dump_to_path, printer


def add_is_guitarist_column_to_schema(package):
    # Add a new field to the first resource
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='is_guitarist',
        type='boolean'
    ))
    # Must yield the modified datapackage
    yield package.pkg
    # And its resources
    yield from package

def add_is_guitarist_column(row):
    row['is_guitarist'] = row['instrument'] == 'guitar'

Flow(
    # Same one as above
    load('beatles.csv'),
    add_is_guitarist_column_to_schema,
    add_is_guitarist_column,
    dump_to_path('beatles_guitarists'),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```


<h3>beatles</h3>



<table>
<thead>
<tr><th>#  </th><th>name
(string)      </th><th>instrument
(string)       </th><th>is_guitarist
(boolean)      </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>john </td><td>guitar</td><td>True </td></tr>
<tr><td>2  </td><td>paul </td><td>bass  </td><td>False</td></tr>
<tr><td>...</td><td>     </td><td>      </td><td>     </td></tr>
<tr><td>4  </td><td>ringo</td><td>drums </td><td>False</td></tr>
</tbody>
</table>





    {'count_of_rows': 4,
     'bytes': 896,
     'hash': 'ae319bad0ad1e345a2a86d8dc9de8375',
     'dataset_name': None}



In this example we create two steps - one for adding the new field (`is_guitarist`) to the schema and another step to modify the actual data.

We can combine the two into one step:


```python
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
    # Take the first resource
    beatles = next(resources)

    # And yield it with with the modification
    def f(row):
        row['is_guitarist'] = row['instrument'] == 'guitar'
        return row

    yield map(f, beatles)

Flow(
    # Same one as above
    load('beatles.csv'),
    add_is_guitarist_column,
    dump_to_path('beatles_guitarists'),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```


<h3>beatles</h3>



<table>
<thead>
<tr><th>#  </th><th>name
(string)      </th><th>instrument
(string)       </th><th>is_guitarist
(boolean)      </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>john </td><td>guitar</td><td>True </td></tr>
<tr><td>2  </td><td>paul </td><td>bass  </td><td>False</td></tr>
<tr><td>...</td><td>     </td><td>      </td><td>     </td></tr>
<tr><td>4  </td><td>ringo</td><td>drums </td><td>False</td></tr>
</tbody>
</table>





    {'count_of_rows': 4,
     'bytes': 896,
     'hash': 'ae319bad0ad1e345a2a86d8dc9de8375',
     'dataset_name': None}



The contract for the `package` processing function is simple:

First modify `package.pkg` (which is a `Package` instance) and yield it.

Then, yield any resources that should exist on the output, with or without modifications.

In the next example we're removing an entire resource in a package processor - this next one filters the list of Academy Award nominees to those who won both the Oscar and an Emmy award:


```python
from dataflows import Flow, load, dump_to_path, checkpoint, printer

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
    
    # important to deque generators to ensure finalization steps of previous processors are executed
    yield from resources

Flow(
    # Emmy award nominees and winners
    load('https://raw.githubusercontent.com/datahq/dataflows/master/data/emmy.csv', name='emmies'),
    # Academy award nominees and winners
    load('https://raw.githubusercontent.com/datahq/dataflows/master/data/academy.csv', encoding='utf8', name='oscars'),
    # save a checkpoint so we won't have to re-download the source data each time
    checkpoint('emmy-academy-nominees-winners'),
    find_double_winners,
    dump_to_path('double_winners'),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```

    saving checkpoint to: .checkpoints/emmy-academy-nominees-winners



<h3>oscars</h3>



<table>
<thead>
<tr><th>#  </th><th>Year
(string)          </th><th style="text-align: right;">   Ceremony
(integer)</th><th>Award
(string)               </th><th style="text-align: right;">  Winner
(string)</th><th>Name
(string)                  </th><th>Film
(string)                           </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>1931/1932</td><td style="text-align: right;"> 5</td><td>Actress       </td><td style="text-align: right;">1</td><td>Helen Hayes      </td><td>The Sin of Madelon Claudet</td></tr>
<tr><td>2  </td><td>1932/1933</td><td style="text-align: right;"> 6</td><td>Actress       </td><td style="text-align: right;">1</td><td>Katharine Hepburn</td><td>Morning Glory             </td></tr>
<tr><td>...</td><td>         </td><td style="text-align: right;">  </td><td>              </td><td style="text-align: right;"> </td><td>                 </td><td>                          </td></tr>
<tr><td>98 </td><td>2015     </td><td style="text-align: right;">88</td><td>Honorary Award</td><td style="text-align: right;">1</td><td>Gena Rowlands    </td><td>                          </td></tr>
</tbody>
</table>


    checkpoint saved: emmy-academy-nominees-winners





    {'count_of_rows': 98,
     'bytes': 6921,
     'hash': '902088336aa4aa4fbab33446a241b5de',
     'dataset_name': None}



Previous flow was a bit complicated, but luckily we have the `join`, `concatenate` and `filter_rows` processors which make such combinations a snap


```python
from dataflows import Flow, load, dump_to_path, join, concatenate, filter_rows, printer, checkpoint

Flow(
    # load from the checkpoint we saved in the previous flow
    checkpoint('emmy-academy-nominees-winners'),
    filter_rows(equals=[dict(winner=1)], resources=['emmies']),
    concatenate(
        dict(emmy_nominee=['nominee'],),
        dict(name='emmies_filtered'),
        resources='emmies'
    ),
    join(
        'emmies_filtered', ['emmy_nominee'],  # Source resource
        'oscars', ['Name'],                   # Target resource
        full=False   # Don't add new fields, remove unmatched rows
    ),
    filter_rows(equals=[dict(Winner='1')]),
    dump_to_path('double_winners'),
    printer(num_rows=1, tablefmt='html')
).process()[1]
```

    using checkpoint data from .checkpoints/emmy-academy-nominees-winners



<h3>oscars</h3>



<table>
<thead>
<tr><th>#  </th><th>Year
(string)          </th><th style="text-align: right;">   Ceremony
(integer)</th><th>Award
(string)               </th><th style="text-align: right;">  Winner
(string)</th><th>Name
(string)                  </th><th>Film
(string)                           </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>1931/1932</td><td style="text-align: right;"> 5</td><td>Actress       </td><td style="text-align: right;">1</td><td>Helen Hayes      </td><td>The Sin of Madelon Claudet</td></tr>
<tr><td>2  </td><td>1932/1933</td><td style="text-align: right;"> 6</td><td>Actress       </td><td style="text-align: right;">1</td><td>Katharine Hepburn</td><td>Morning Glory             </td></tr>
<tr><td>...</td><td>         </td><td style="text-align: right;">  </td><td>              </td><td style="text-align: right;"> </td><td>                 </td><td>                          </td></tr>
<tr><td>98 </td><td>2015     </td><td style="text-align: right;">88</td><td>Honorary Award</td><td style="text-align: right;">1</td><td>Gena Rowlands    </td><td>                          </td></tr>
</tbody>
</table>





    {'count_of_rows': 98,
     'bytes': 6921,
     'hash': '902088336aa4aa4fbab33446a241b5de',
     'dataset_name': None}



## Builtin Processors

DataFlows comes with a few built-in processors which do most of the heavy lifting in many common scenarios, leaving you to implement only the minimum code that is specific to your specific problem.

A complete list, which also includes an API reference for each one of them, can be found in the [Built-in Processors](https://github.com/datahq/dataflows/blob/master/PROCESSORS.md#builtin-processors) page.

## Nested Flows

The flow object itself can be used as a step in another flow, this allows for useful design patterns which promote code reusability and readability:


```python
from dataflows import Flow, printer

# generate a customizable, predefined flow
def text_processing_flow(star_letter_idx):

    # run upper on all cell values
    def upper(row):
        for k in row:
            row[k] = row[k].upper()
    
    # star the letter at the index from star_letter_idx argument
    def star_letter(row):
        for k in row:
            s = list(row[k])
            s[star_letter_idx] = '*'
            row[k] = ''.join(s)
    
    def print_foo(row):
        print('  '.join(list(row['foo'])))

    return Flow(upper, star_letter, print_foo)

Flow(
    [{'foo': 'bar'},
     {'foo': 'bax'}],
    text_processing_flow(0),
    text_processing_flow(1),
    text_processing_flow(2),
).process()[1]
```

    *  A  R
    *  *  R
    *  *  *
    *  A  X
    *  *  X
    *  *  *





    {}



## Next Steps

* [DataFlows Processors Reference](https://github.com/datahq/dataflows/blob/master/PROCESSORS.md)
* [Datapackage Pipelines Tutorial](https://github.com/frictionlessdata/datapackage-pipelines/blob/master/TUTORIAL.ipynb) - Use the flows as building blocks for more complex pipelines processing systems.


```python

```
