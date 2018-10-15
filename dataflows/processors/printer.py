from tabulate import tabulate
from ..helpers.resource_matcher import ResourceMatcher


try:
    from IPython.core.display import display, HTML
    get_ipython
    def display_html(data):
        display(HTML(data))
except (NameError, ImportError):
    def display_html(data):
        print(data)


def _header_print(header, kwargs):
    if kwargs.get('tablefmt') == 'html':
        display_html(f'<h3>{header}</h3>')
    else:
        print(f'{header}:')


def _table_print(data, kwargs):
    if kwargs.get('tablefmt') == 'html':
        display_html(data)
    else:
        print(data)


def printer(num_rows=10, last_rows=None, fields=None, resources=None,
            header_print=_header_print, table_print=_table_print, **kwargs):

    def func(rows):
        spec = rows.res

        if not ResourceMatcher(resources, spec.descriptor).match(spec.name):
            yield from rows
            return

        header_print(spec.name, kwargs)

        schema_fields = spec.schema.fields
        if fields:
            schema_fields = [f for f in schema_fields if f.name in fields]

        field_names = [f.name for f in schema_fields]
        headers = ['#'] + [
            '{}\n({})'.format(f.name, f.type) for f in schema_fields
        ]
        toprint = []
        last = []
        x = 1

        for i, row in enumerate(rows):
            yield row

            index = i + 1
            row = [index] + [row[f] for f in field_names]

            if index - x == (num_rows + 1):
                x *= num_rows

            if 0 <= index - x <= num_rows:
                last.clear()
                if toprint and toprint[-1][0] != index - 1:
                    toprint.append(['...'])
                toprint.append(row)
            else:
                last.append(row)
                if len(last) > (last_rows or num_rows):
                    last = last[1:]

        if toprint and last and toprint[-1][0] != last[0][0] - 1:
            toprint.append(['...'])

        toprint += last

        table_print(tabulate(toprint, headers=headers, **kwargs), kwargs)

    return func
