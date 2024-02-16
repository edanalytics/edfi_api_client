import time


def format_print(text: str):
    message = f"\n##### {text} #####\n"
    print(message)


def time_it(func, wrap_func=None, *args, **kwargs):
    """
    Helper for retrieving runtime of a function.
    Return runtime (in seconds), followed by function return.

    ```
    runtime, return_value = time_it(function, arguments)
    ```

    :return:
    """
    start = time.time()
    return_val = func(*args, **kwargs)
    if wrap_func:
        return_val = wrap_func(return_val)
    end = time.time()

    runtime = round(end - start, 2)
    return runtime, return_val