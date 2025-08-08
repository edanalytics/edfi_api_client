from edfi_api_client import EdFiClient
import os
import logging
import pytest
from requests import HTTPError

logging.basicConfig(level=logging.DEBUG)


def test_mutually_exclusive_authentication_methods():
    # not permitted to pass in client key and external token at the same time
    with pytest.raises(ValueError):
        bad_api = EdFiClient(
            base_url='https://localhost/api',
            client_key='testkey',
            client_secret='testsecret',
            access_token='testtoken'
        )

def test_external_token_string():
    base_url = os.environ.get('EDFI_API_BASE_URL', 'https://localhost/api')
    client_key = os.environ.get('EDFI_API_CLIENT_KEY', 'testkey')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')

    # setup client that will authenticate on its own
    parent_api = EdFiClient(
        base_url=base_url,
        client_key=client_key,
        client_secret=client_secret
    )

    # make call that requires authentication (sessions are lazy)
    parent_school_count = parent_api.resource('schools').get_total_count()
    logging.info(f'Parent API yields {parent_school_count}')
    common_token = parent_api.session.access_token

    # setup client passing in the existing token
    child_api = EdFiClient(
        base_url=base_url,
        access_token=common_token
    )
    
    # make same call requiring authentication
    child_school_count = child_api.resource('schools').get_total_count()
    logging.info(f'Child API yields {child_school_count}')
    
    assert child_school_count == parent_school_count


def test_external_token_getter():
    base_url = os.environ.get('EDFI_API_BASE_URL', 'https://localhost/api')
    client_key = os.environ.get('EDFI_API_CLIENT_KEY', 'testkey')
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', 'testsecret')

    # setup client that will authenticate on its own
    parent_api = EdFiClient(
        base_url=base_url,
        client_key=client_key,
        client_secret=client_secret
    )

    # make call that requires authentication (sessions are lazy)
    parent_school_count = parent_api.resource('schools').get_total_count()
    logging.info(f'Parent API yields {parent_school_count}')
    common_payload = parent_api.session.last_auth_payload

    # setup client passing in the existing token (as a getter)
    child_api = EdFiClient(
        base_url=base_url,
        access_token=lambda: common_payload
    )
    
    # make same call requiring authentication
    child_school_count = child_api.resource('schools').get_total_count()
    logging.info(f'Child API yields {child_school_count}')
    
    assert child_school_count == parent_school_count


def test_token_getter_called_on_retry():
    '''Important if we want paged requests to be able to fetch a new token 
    from the external token getter in the middle'''

    base_url = os.environ.get('EDFI_API_BASE_URL', 'https://localhost/api')
    
    # pass in a bad token that will result in 401 errors
    counter = 0
    def token_getter():
        nonlocal counter 
        counter += 1
        logging.info(f'Token getter called {counter} times')
        return {'access_token': 'badtoken'}

    api = EdFiClient(
        base_url=base_url,
        access_token=token_getter
    )

    try:
        # call that requires authentication
        api.resource('schools').get_total_count(max_retries=2, retry_on_failure=True, max_wait=2)
    except HTTPError:
        # max_retries actually means max total tries; 1 try and 1 retry
        assert counter == 2
        

        
