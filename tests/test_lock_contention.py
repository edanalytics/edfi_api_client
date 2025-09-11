import os
import time
import logging
import random
from multiprocessing import Pool
from collections import Counter

import pytest
import portalocker

from edfi_api_client import EdFiClient

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s'
)

def get_token(i):
    base_url = os.environ.get('EDFI_API_BASE_URL')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')
    client_key = os.environ.get('EDFI_API_CLIENT_KEY', 'testkey')

    if any([x is None for x in [base_url, client_secret, client_key]]):
        raise ValueError('Please provide valid credentials')
    
    try:
        api = EdFiClient(
            base_url=base_url, 
            client_key=client_key, 
            client_secret=client_secret,
            use_token_cache=True
        )

        # call requiring auth
        _ = api.resource('schools').get_total_count()
        token_prefix = api.session.access_token[:5]
    except Exception as e:
        logging.info(e)
        token_prefix = None

    return token_prefix

def test_multiprocessing_uses_same_token():
    with Pool(25) as p:
        results = p.map(get_token, range(100))
    logging.info(Counter(results))
    assert len(set(results)) == 1

def test_multiprocessing_with_forced_refreshes():
    # create API client (unauthenticated) to pull token cache path
    base_url = os.environ.get('EDFI_API_BASE_URL')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')
    client_key = os.environ.get('EDFI_API_CLIENT_KEY', 'testkey')

    api = EdFiClient(
        base_url=base_url, 
        client_key=client_key, 
        client_secret=client_secret,
        use_token_cache=True
    )
    token_path = api.session.token_cache_path


    with Pool(25) as p:
        results = p.map_async(get_token, range(100))

        # force cache invalidations to simulate token expiry
        refresh_counter = 0
        while not results.ready():
            time.sleep(random.randint(1, 10))
            if os.path.exists(token_path):
                os.remove(token_path)
            refresh_counter += 1
                

        tokens = results.get()
        
        logging.info(f"Invalided token cache {refresh_counter} times, total of {len(set(tokens))} seen")
        logging.info(Counter(tokens))

        # worst case every invalidation forces a new token 
        assert(len(set(tokens)) <= refresh_counter + 1)   

