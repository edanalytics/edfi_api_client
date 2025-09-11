from edfi_api_client import EdFiClient
import os
import logging
from multiprocessing import Pool
import pytest

logging.basicConfig(level=logging.DEBUG)

def get_token(i):
    base_url = os.environ.get('EDFI_API_BASE_URL')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')
    client_key = os.environ.get('EDFI_API_CLIENT_KEY', 'testkey')
    api = EdFiClient(
        base_url=base_url, 
        client_key=client_key, 
        client_secret=client_secret,
        use_token_cache=True
    )
    _ = api.resource('schools').get_total_count()

    return api.session.access_token[:5]

def test_multiprocessing_uses_same_token():
    with Pool(25) as p:
        results = p.map(get_token, range(100))
    logging.info(results)
    assert len(set(results)) == 1
