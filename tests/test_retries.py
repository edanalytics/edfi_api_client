from edfi_api_client import EdFiClient

import pytest
from requests.auth import _basic_auth_str
from requests.exceptions import HTTPError
import responses
from responses import matchers

import json

# Dummy values; all requests should be mocked
BASE_URL = 'http://localhost'
CLIENT_KEY = 'client_key'
CLIENT_SECRET = 'client_secret'
BASIC_AUTH_HEADER = _basic_auth_str(CLIENT_KEY, CLIENT_SECRET)
TOKEN = 'token'
INSTANCE_CODE = 'instance_code'

@responses.activate
def test_max_retries():
    """Test that max retries applies to each page"""
    responses.get(
        BASE_URL,
        json={
            'version': '7.1',
            'informationalVersion': '7.1',
            'suite': '3',
            'build': '2025.5.1.1636',
            'apiMode': 'District Specific',
            'dataModels': [{'informationalVersion': 'The Ed-Fi Data Model 5.0',
                 'name': 'Ed-Fi',
                 'version': '5.0.0'}],
            'urls': {
                'dependencies': f'{BASE_URL}/metadata/data/v3/dependencies',
                'openApiMetadata': f'{BASE_URL}/metadata/',
                'oauth': f'{BASE_URL}/oauth/token',
                'dataManagementApi': f'{BASE_URL}/data/v3/',
                'xsdMetadata': f'{BASE_URL}/metadata/xsd'
            }
        }
    )
    responses.post(
        f'{BASE_URL}/oauth/token',
        json={
            "access_token": TOKEN,
            "expires_in": 1800,
            "token_type": "bearer"
        },
        match=[
            matchers.header_matcher({"Authorization": BASIC_AUTH_HEADER})
        ]
    )
    responses.post(
        f'{BASE_URL}/oauth/token_info',
        json={
            "active": True,
            "client_id": CLIENT_KEY,
            "assigned_profiles": [],
            "education_organizations": [
                {
                    'education_organization_id': 9999,
                    'local_education_agency_id': 9999,
                    'name_of_institution': 'District1',
                    'state_education_agency_id': 1,
                    'type': 'edfi.LocalEducationAgency'
                }
            ],
            "namespace_prefixes": ['uri://ed-fi.org/']
        },
        match=[
            matchers.urlencoded_params_matcher({'token': TOKEN}),
            matchers.header_matcher({"Authorization": f"Bearer {TOKEN}"})
        ]
    )

    # mock resource paging; force HTTP errors to max out retries
    total_calls = 0
    page_index = 1
    retry_index = 1
    max_retries = 5
    max_pages = 2
    def school_callback(request):
        nonlocal total_calls, page_index, retry_index
        total_calls += 1
        if retry_index < max_retries:
            # respond with 504 errors to force retry
            retry_index += 1
            return (504, {}, json.dumps({'error': 'Timed out.'}))
        else:
            # on last try, respond with rows (if there are rows left)
            retry_index = 1
            if page_index <= max_pages:
                response = (200, {}, json.dumps([{'id': page_index}]))
                page_index += 1
                return response
            else:
                # or return an empty array
                return(200, {}, json.dumps([]))
    responses.add_callback(
        responses.GET,
        f'{BASE_URL}/data/v3/ed-fi/schools',
        callback=school_callback,
        content_type='application/json'
    )


    client = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET)
    try:
        client.connect(retry_on_failure=True, max_retries=max_retries, max_wait=1) # decrease max_wait to speed up testing
        schools = list(client.resource('schools').get_rows(page_size=1))
    except TypeError: # old signature where retry parameters are in get_rows
        schools = list(client.resource('schools').get_rows(page_size=1, retry_on_failure=True, max_retries=max_retries, max_wait=1))


    assert(len(schools) == max_pages)
    assert(total_calls == max_retries * (max_pages+1)) # max_retries for the last, empty page as well


@responses.activate()
def test_default_no_retry():
    """Test that retries are off by default"""
    responses.get(
        BASE_URL,
        json={
            'version': '7.1',
            'informationalVersion': '7.1',
            'suite': '3',
            'build': '2025.5.1.1636',
            'apiMode': 'District Specific',
            'dataModels': [{'informationalVersion': 'The Ed-Fi Data Model 5.0',
                 'name': 'Ed-Fi',
                 'version': '5.0.0'}],
            'urls': {
                'dependencies': f'{BASE_URL}/metadata/data/v3/dependencies',
                'openApiMetadata': f'{BASE_URL}/metadata/',
                'oauth': f'{BASE_URL}/oauth/token',
                'dataManagementApi': f'{BASE_URL}/data/v3/',
                'xsdMetadata': f'{BASE_URL}/metadata/xsd'
            }
        }
    )
    responses.post(
        f'{BASE_URL}/oauth/token',
        json={
            "access_token": TOKEN,
            "expires_in": 1800,
            "token_type": "bearer"
        },
        match=[
            matchers.header_matcher({"Authorization": BASIC_AUTH_HEADER})
        ]
    )
    responses.post(
        f'{BASE_URL}/oauth/token_info',
        json={
            "active": True,
            "client_id": CLIENT_KEY,
            "assigned_profiles": [],
            "education_organizations": [
                {
                    'education_organization_id': 9999,
                    'local_education_agency_id': 9999,
                    'name_of_institution': 'District1',
                    'state_education_agency_id': 1,
                    'type': 'edfi.LocalEducationAgency'
                }
            ],
            "namespace_prefixes": ['uri://ed-fi.org/']
        },
        match=[
            matchers.urlencoded_params_matcher({'token': TOKEN}),
            matchers.header_matcher({"Authorization": f"Bearer {TOKEN}"})
        ]
    )
    responses.get(
        f'{BASE_URL}/data/v3/ed-fi/schools',
        json={'error': 'Timed out.'},
        status=504,
        content_type='application/json'
    )

    client = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET)
    with pytest.raises(HTTPError, match=r".*time-out.*"):
        schools = list(client.resource('schools').get_rows())


