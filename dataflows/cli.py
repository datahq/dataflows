import os

import slugify 
import click
from jinja2 import Environment, PackageLoader
import inquirer
from inquirer import themes

# Some settings
FORMATS = ['csv', 'tsv', 'xls', 'xlsx', 'json', 'ndjson', 'ods', 'gsheet']
INPUTS = dict((
  ('File', 'file'),
  ('Remote URL', 'remote'), 
  ('SQL Database', 'sql'),
  ('Other', 'other')
))
PROCESSING = dict((
  ('Sort all rows by key',                'sort'),
  ('Filter according to column values',   'filter'),
  ('Search & replace values in the data', 'find_replace'),
  ('Delete some columns',                 'data_fields'),
  ('Normalize and validate numbers, dates and other types', 'set_type'),
  ('Un-pivot the data',                   'unpivot'),
  ('Custom row-by-row processing',        'custom'),
))
OUTPUTS = dict((
  ('Just print the data',                     'print'),
  ('As a Python list',                        'list'),
  ('A CSV file (in a data package)',          'dp_csv'),
  ('A CSV file (in a zipped data package)',   'dp_csv_zip'),
  ('A JSON file (in a data package)',         'dp_csv'),
  ('A JSON file (in a zipped data package)',  'dp_csv_zip'),
  ('An SQL database',                         'sql'),
))

# Utility functions
def fall(*validators):
  def func(*args):
    return all(v(*args) for v in validators)
  return func


def fany(*validators):
  def func(*args):
    return any(v(*args) for v in validators)
  return func


# Validators
def valid_url(ctx, url):
  return url.startswith('http://') or url.startswith('https://')

def not_empty(ctx, x):
  return x

# Converters
def convert_processing(ctx, key):
  ctx['processing'] = [PROCESSING[x] for x in key]
  return True


def convert_input(ctx, key):
  ctx['input'] = INPUTS[key]
  return True


def convert_output(ctx, key):
  ctx['output'] = OUTPUTS[key]
  return True


def extract_format(ctx, url):
  if url:
    _, ext = os.path.splitext(url)
    if ext:
      ctx['format'] = ext[1:].lower()
      return True
  ctx['format'] = None
  return True

# Render
env = Environment(loader=PackageLoader('dataflows'),
                  autoescape=False)
def render(parameters):
  env.get_template('main.tpl')
  return ''

# Main CLI routine
@click.command()
def init():
  input("""Hi There!
DataFlows will now bootstrap a data processing flow based on your needs.

Press any key to start...
""")

  questions = [
    # Input
    inquirer.List('input_str', 
                  message='What is the source of your data?', 
                  choices=INPUTS.keys(),
                  validate=convert_input),

    # Input Parameters
    inquirer.Text('input_url', 
                  message="What is the path of that file",
                  ignore=lambda ctx: ctx['input'] != 'file',
                  validate=fall(not_empty, extract_format)),
    inquirer.List('format', 
                  message="We couldn't detect the file format - which is it?",
                  choices=FORMATS[:-1],
                  ignore=fany(lambda ctx: ctx['input'] != 'file',
                              lambda ctx: ctx.get('format') in FORMATS)),

    inquirer.Text('input_url', 
                  message="Where is that file located (URL)",
                  ignore=lambda ctx: ctx['input'] != 'remote',
                  validate=fall(extract_format, not_empty, valid_url)),
    inquirer.List('format', 
                  message="We couldn't detect the source format - which is it",
                  choices=FORMATS,
                  ignore=fany(lambda ctx: ctx['input'] != 'remote',
                              lambda ctx: ctx.get('format') in FORMATS)),

    inquirer.Text('sheet', 
                  message="Which sheet in the spreadsheet should be processed (name or index)",
                  validate=not_empty,
                  ignore=lambda ctx: ctx.get('format') not in ('xls', 'xlsx', 'ods')),

    inquirer.Text('input_url', 
                  message="What is the connection string to the database",
                  validate=not_empty,
                  ignore=lambda ctx: ctx['input'] != 'sql'),
    inquirer.Text('input_db_table', 
                  message="...and the name of the database table to extract",
                  validate=not_empty,
                  ignore=lambda ctx: ctx['input'] != 'sql'),

    inquirer.Text('input_url', 
                  message="Describe that other source (shortly)",
                  ignore=lambda ctx: ctx['input'] != 'other'),

    # Processing
    inquirer.Checkbox('processing_str',
                      message="What kind of processing would you like to run on the data",
                      choices=PROCESSING.keys(),
                      validate=convert_processing),

    # Output
    inquirer.List('output_str',
                  message="Finally, where would you like the output data",
                  choices=OUTPUTS.keys(),
                  validate=convert_output),
    inquirer.Text('output_url', 
                  message="What is the connection string to the database",
                  validate=not_empty,
                  ignore=lambda ctx: ctx['output'] != 'sql'),
    inquirer.Text('output_db_table', 
                  message="...and the name of the database table to write to",
                  validate=not_empty,
                  ignore=lambda ctx: ctx['output'] != 'sql'),

    # Finalize
    inquirer.Text('title', 
                  message="That's it! Now, just provide a title for your processing flow",
                  validate=not_empty),
]
  answers = inquirer.prompt(questions, answers=dict(a=1), theme=themes.GreenPassion())
  answers['slug'] = slugify.slugify(answers['title'], separator='_')
  render(answers)

  answers = inquirer.prompt([
    inquirer.Confirm('edit',
                     message='Would you like to open {} in the default editor?'.format(filename),
                     default=False)
  ])
  if answers['edit']:
    click.edit(filename='cli.py')
  print('Done!')


if __name__ == '__main__':
  init()