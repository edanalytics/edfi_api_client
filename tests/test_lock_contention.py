import os
import time
import logging
import random
from multiprocessing import Pool
from collections import Counter

import pytest

from edfi_api_client import EdFiClient
from edfi_api_client.token_cache import LockfileTokenCache

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s'
)

def create_client_from_env():
    """Instantiates an EdFiClient with a pure-Python lockfile token cache"""
    base_url = os.environ.get('EDFI_API_BASE_URL')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')
    client_key = os.environ.get('EDFI_API_CLIENT_ID', 'testkey')

    if any([x is None for x in [base_url, client_secret, client_key]]):
        raise ValueError('Please provide valid credentials')

    api = EdFiClient(
        base_url=base_url, 
        client_key=client_key, 
        client_secret=client_secret,
        token_cache=LockfileTokenCache()
    )

    return api

def create_client_and_get_token(i):
    api = create_client_from_env()
    
    try:
        # call requiring auth
        _ = api.resource('schools').get_total_count()
        token_prefix = api.session.access_token[:5]
    except Exception as e:
        logging.error(e, exc_info=True)
        token_prefix = None

    return token_prefix

def test_multiprocessing_uses_same_token():
    with Pool(25) as p:
        tokens = p.map(create_client_and_get_token, range(100))
    token_counts = Counter(tokens)
    logging.info(token_counts)
    assert (
        len(token_counts) == 1 and
        None not in token_counts
    )

def test_multiprocessing_with_forced_refreshes():
    # create API client (unauthenticated) to pull token cache path
    api = create_client_from_env()
    token_path = api.session.token_cache.cache_path

    with Pool(25) as p:
        results = p.map_async(create_client_and_get_token, range(100))

        # force cache invalidations to simulate token expiry
        refresh_counter = 0
        while not results.ready():
            time.sleep(random.randint(1, 10)/5)
            if os.path.exists(token_path):
                os.remove(token_path)
            refresh_counter += 1
                
        tokens = results.get()
        token_counts = Counter(tokens)
        
        logging.info(f"Invalided token cache {refresh_counter} times, total of {len(token_counts)} seen")
        logging.info(token_counts)

        # worst case every invalidation forces a new token 
        assert(
            len(token_counts) <= refresh_counter + 1 and
            None not in token_counts
        )   

