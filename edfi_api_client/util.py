import json
import re

from typing import List, Union

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

def page_to_bytes(page: List[dict]) -> bytes:
    return b''.join(map(lambda row: json.dumps(row).encode('utf-8') + b'\n', page))

def clean_post_row(row: Union[str, dict]) -> str:
    """
    Remove 'id' from a string or dictionary payload and force to a string.
    TODO: Can this be made more efficient?
    :return:
    """
    if isinstance(row, (bytes, str)):
        row = json.loads(row)

    # "Resource identifiers cannot be assigned by the client."
    if 'id' in row:
        del row['id']

    return json.dumps(row)

def url_join(*args) -> str:
    return '/'.join(
        map(lambda x: str(x).rstrip('/'), filter(lambda x: x is not None, args))
    )
