import json
import re

from typing import List


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

def plural_to_singular(name: str) -> str:
    """
    Convert a (Ed-Fi resource) name from plural to singular.
    TODO: Genericize this to handle all edge-cases.

    :param name:
    :return:
    """
    if name == "people":
        return "person"

    if name.endswith('ies'):  # e.g. families -> family
        return name[0:-3] + "y"

    if name.endswith('s'):  # Remove 's' at the end.
        return name[0:-1]

    raise Exception(f"Name has irregular plural form: {name}")

def page_to_bytes(page: List[dict]) -> bytes:
    return b''.join(map(lambda row: json.dumps(row).encode('utf-8') + b'\n', page))

def url_join(*args) -> str:
    return '/'.join(
        map(lambda x: str(x).rstrip('/'), filter(lambda x: x is not None, args))
    )
