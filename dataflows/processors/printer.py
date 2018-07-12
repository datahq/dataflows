from tabulate import tabulate


def printer():

    def func(rows):
        spec = rows.res
        print('{}:'.format(spec.name))

        field_names = [f.name for f in spec.schema.fields]
        headers = ['#'] + [
            '{}\n({})'.format(f.name, f.type) for f in spec.schema.fields
        ]
        toprint = []
        last = []
        x = 1

        for i, row in enumerate(rows):
            yield row

            index = i + 1
            row = [index] + [row[f] for f in field_names]

            if index - x == 11:
                x *= 10

            if 0 <= index - x <= 10:
                last.clear()
                if toprint and toprint[-1][0] != index - 1:
                    toprint.append(['...'])
                toprint.append(row)
            else:
                last.append(row)
                if len(last) > 10:
                    last = last[1:]

        if toprint and last and toprint[-1][0] != last[0][0] - 1:
            toprint.append(['...'])

        toprint += last
        print(tabulate(toprint, headers=headers))

    return func
