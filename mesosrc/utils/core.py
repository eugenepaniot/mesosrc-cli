import datetime


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


def flatten_dict(d):
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value

    return dict(items())


def truncate_string(string, lenght=1024):
    return string[:int(lenght / 2)] + '  ...  (TRUNCATED (' + str(lenght) + ') OUTPUT)  ...  ' + string[-int(lenght / 2):] \
        if string and len(string) > int(lenght) else string


def sec2time(sec):
    if hasattr(sec, '__len__'):
        return [sec2time(s) for s in sec]

    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    pattern = r'%02dh %02dm %02ds'

    if d == 0:
        return pattern % (h, m, s)

    return ('%dd, ' + pattern) % (d, h, m, s)


def format_nanos(nanos):
    dt = datetime.datetime.fromtimestamp(nanos / 1e9)
    return '{}'.format(dt.strftime('%Y-%m-%dT%H:%M:%S'), nanos % 1e9)


def to_bool(value):
    valid = {'true': True, 't': True, '1': True,
             'false': False, 'f': False, '0': False,
             }

    if isinstance(value, bool):
        return value

    if type(value) is not str:
        raise ValueError('invalid literal for boolean. Not a string.')

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)
