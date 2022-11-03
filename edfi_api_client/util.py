import datetime
import re


def camel_to_snake(name: str) -> str:
    """
    Convert camelCase names to snake_case names.
    Ed-Fi endpoints are camelCase, but ingests use snake_case.
    :param name: A camelCase string value to be converted to snake_case.
    :return: A string in snake_case.
    """
    name = re.sub(r'(.)([A-Z][a-z]+)' , r'\1_\2', name)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = re.sub(r'[_ ]+', '_', name)
    return name.lower()


def snake_to_camel(name: str) -> str:
    """
    Convert snake_case names to camelCase names.
    Python arguments are snake_case, but the Ed-Fi API uses camelCase.
    :param name: A snake_case string value to be converted to camelCase.
    :return: A string in camelCase.
    """
    words = re.split(r"[_-]+", name)
    return words[0] + ''.join(word.title() for word in words[1:])


def url_join(*args) -> str:
    return '/'.join(
        map(lambda x: str(x).rstrip('/'), filter(lambda x: x is not None, args))
    )


def seconds_to_text(seconds: int) -> str:
    """

    :param seconds:
    :return:
    """
    delta_str = str(datetime.timedelta(seconds=seconds))
    hours, minutes, seconds = list(map(int, delta_str.split(':')))

    time_strings = []

    if hours:
        desc = "hours" if hours > 1 else "hour"
        time_strings.append( f"{hours} {desc}")
    if minutes:
        desc = "minutes" if minutes > 1 else "minute"
        time_strings.append( f"{minutes} {desc}" )
    if seconds:
        desc = "seconds" if seconds > 1 else "second"
        time_strings.append( f"{seconds} {desc}" )

    return ", ".join(time_strings)
