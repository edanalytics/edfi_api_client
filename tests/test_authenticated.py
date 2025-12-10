from edfi_api_client import EdFiClient

import pytest
from requests.auth import _basic_auth_str
import responses
from responses import matchers

# Dummy values; all requests should be mocked
BASE_URL = 'http://localhost'
CLIENT_KEY = 'client_key'
CLIENT_SECRET = 'client_secret'
BASIC_AUTH_HEADER = _basic_auth_str(CLIENT_KEY, CLIENT_SECRET)
TOKEN = 'token'
INSTANCE_CODE = 'instance_code'


@pytest.fixture
def base_url_response():
    """Mock response to root endpoint; clients will hit this to get apiMode"""
    return responses.get(
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


@responses.activate
def test_get_token_info(base_url_response):
    """Test that client.get_token_info() POSTs with a valid token from auth"""
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
            matchers.json_params_matcher({'token': TOKEN}),
            matchers.header_matcher({"Authorization": f"Bearer {TOKEN}"})
        ]
    )

    client = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET)
    token_info = client.get_token_info()
    assert(token_info.get('client_id') == CLIENT_KEY)

@responses.activate
def test_instance_specific_connect(base_url_response):
    """Test that a client started in instance-year specific mode hits the right oauth/token URL"""
    responses.post(
        f'{BASE_URL}/{INSTANCE_CODE}/oauth/token',
        json={
            "access_token": TOKEN,
            "expires_in": 1800,
            "token_type": "bearer"
        },
        match=[
            matchers.header_matcher({"Authorization": BASIC_AUTH_HEADER})
        ]
    )

    client = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET, api_mode='instance_year_specific', instance_code=INSTANCE_CODE, api_year=2025)
    session = client.connect()
    assert(session)


@responses.activate
def test_multiple_tokens_when_no_cache(base_url_response):
    """Test that two clients with no token cache will pull different tokens"""
    responses.post(
        f'{BASE_URL}/{INSTANCE_CODE}/oauth/token',
        json={
            "access_token": 'first',
            "expires_in": 1800,
            "token_type": "bearer"
        },
        match=[
            matchers.header_matcher({"Authorization": BASIC_AUTH_HEADER})
        ]
    )
    responses.post(
        f'{BASE_URL}/{INSTANCE_CODE}/oauth/token',
        json={
            "access_token": 'second',
            "expires_in": 1800,
            "token_type": "bearer"
        },
        match=[
            matchers.header_matcher({"Authorization": BASIC_AUTH_HEADER})
        ]
    )

    client_1 = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET, api_mode='instance_year_specific', instance_code=INSTANCE_CODE, api_year=2025)
    client_2 = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET, api_mode='instance_year_specific', instance_code=INSTANCE_CODE, api_year=2025)

    assert(client_1.session.access_token == 'first')
    assert(client_2.session.access_token == 'second')

