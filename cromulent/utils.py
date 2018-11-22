import functools

def memoize(func):
    cache = {}

    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return memoized_func

def parse_wf_report_opts(opts=None):
    if opts is None:
        return {}

    opts_params = opts.split(';')
    params = {}

    for o in opts_params:
        (k, v) = o.split('=')
        params[k] = v

    return params
