import requests

from requests.exceptions import HTTPError
from typing import Optional

from edfi_api_client import util
from edfi_api_client.edfi_endpoint import EdFiResource, EdFiDescriptor, EdFiComposite
from edfi_api_client.edfi_swagger import EdFiSwagger
from edfi_api_client.session import EdFiSession


import logging
logging.basicConfig(
    level="WARNING",
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class EdFiClient:
    """
    Client for interacting with the Ed-Fi API.
    Includes methods for authentication, parsing the Swagger, and getting content from endpoints.

    :param base_url: The root URL of the API, without components like `data/v3`
    :param client_key: Authentication key
    :param client_secret: Authentication secret
    :param api_version: 3 for Suite 3, 2 for older 2.x instances
    :param api_mode: ['shared_instance', 'sandbox', 'district_specific', 'year_specific', 'instance_year_specific']
    :param api_year: Required only for 'year_specific' or 'instance_year_specific' modes
    :param instance_code: Only required for 'instance_specific' or 'instance_year_specific modes'
    """

    def __init__(self,
        base_url     : str,
        client_key   : Optional[str] = None,
        client_secret: Optional[str] = None,

        *,
        api_version  : int = 3,
        api_mode     : Optional[str] = None,
        api_year     : Optional[int] = None,
        instance_code: Optional[str] = None,

        verify_ssl   : bool = True,
        verbose      : bool = False,
    ):
        # Update logger first
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        self.base_url = base_url
        self.client_key = client_key
        self.client_secret = client_secret
        self.verify_ssl = verify_ssl
        self.access_token: Optional[str] = None

        self.api_version = int(api_version)
        self.api_mode = api_mode or self.get_api_mode()
        self.api_year = api_year
        self.instance_code = instance_code

        # Build endpoint URL pieces
        self.version_url_string = "data/v3"

        if self.api_version == 2:
            raise NotImplementedError(
                "Ed-Fi 2 functionality has been deprecated. Use `pip install edfi_api_client~=0.2.0` for Ed-Fi 2 ODSes."
            )

        # Swagger variables for populating resource metadata (retrieved lazily)
        self.swaggers = {
            'resources'  : None,
            'descriptors': None,
            'composites' : None,
        }
        self._resources   = None
        self._descriptors = None

        # Initialize lazy session object (do not connect until an ODS-request method is called)
        oauth_url = util.url_join(self.base_url, 'oauth/token')
        self.session = EdFiSession(oauth_url, self.client_key, self.client_secret)


    def __repr__(self):
        """
        (Un)Authenticated Ed-Fi(3) Client [{api_mode}]
        """
        _session_string = "Authenticated" if self.session else "Unauthenticated"
        _api_mode = util.snake_to_camel(self.api_mode) if self.api_mode else "None"
        if self.api_year:
            _api_mode += f" {self.api_year}"

        return f"<{_session_string} Ed-Fi{self.api_version} API Client [{_api_mode}]>"


    ### Methods for accessing the Base URL payload and Swagger
    def get_info(self) -> dict:
        """
        Ed-Fi3 returns a helpful payload from the base URL.
        Note: This method should not be used for Ed-Fi2; no standardized payload is returned.

        {
            'version': '5.2',
            'informationalVersion': '5.2',
            'suite': '3',
            'build': '5.2.14406.0',
            'apiMode': 'District Specific',
            'dataModels': [
                {'name': 'Ed-Fi', 'version': '3.3.0-a'}
            ],
            'urls': {
                'dependencies': '{BASE_URL}/metadata/data/v3/dependencies',
                'openApiMetadata': '{BASE_URL}/metadata/',
                'oauth': '{BASE_URL}/oauth/token',
                'dataManagementApi': '{BASE_URL}/data/v3/',
                'xsdMetadata': '{BASE_URL}/metadata/xsd'
            }
        }

        :return: The descriptive payload returned by the API host.
        """
        return requests.get(self.base_url, verify=self.verify_ssl).json()


    def get_api_mode(self) -> Optional[str]:
        """
        Retrieve api_mode from the metadata exposed at the API root.

        :return:
        """
        api_mode = self.get_info().get('apiMode')
        return util.camel_to_snake(api_mode) if api_mode else None


    def get_ods_version(self) -> Optional[str]:
        """
        Retrieve ods_version from the metadata exposed at the API root.
        :return:
        """
        return self.get_info().get('version')


    def get_data_model_version(self) -> Optional[str]:
        """
        Retrieve Ed-Fi data model version from the metadata exposed at the API root.
        :return:
        """
        data_models = self.get_info().get('dataModels', [])

        for data_model_dict in data_models:
            if data_model_dict.get('name') == 'Ed-Fi':
                return data_model_dict.get('version')
        else:
            return None


    ### Methods related to retrieving the Swagger or attributes retrieved therein
    def get_swagger(self, component: str = 'resources') -> EdFiSwagger:
        """
        OpenAPI Specification describes the entire Ed-Fi API surface in a
        JSON payload.
        Can be used to surface available endpoints.

        :param component: Which component's swagger spec should be retrieved?
        :return: Swagger specification definition, as a dictionary.
        """
        swagger_url = util.url_join(
            self.base_url, 'metadata', self.version_url_string, component, 'swagger.json'
        )

        payload = requests.get(swagger_url, verify=self.verify_ssl).json()
        swagger = EdFiSwagger(component, payload)

        # Save the swagger in memory to save time on subsequent calls.
        self.swaggers[component] = swagger
        return swagger

    def _set_swagger(self, component: str):
        """
        Populate the respective swagger object in `self.swaggers` if not already populated.

        :param component:
        :return:
        """
        if self.swaggers.get(component) is None:
            logging.info(f"[Get {component.title()} Swagger] Retrieving Swagger into memory...")
            self.get_swagger(component)


    @property
    def resources(self):
        """

        :return:
        """
        if self._resources is None:
            self._set_swagger('resources')
            self._resources = self.swaggers['resources'].endpoints
        return self._resources

    @property
    def descriptors(self):
        """

        :return:
        """
        if self._descriptors is None:
            self._set_swagger('descriptors')
            self._descriptors = self.swaggers['descriptors'].endpoints
        return self._descriptors


    ### Helper methods for building elements of endpoint URLs for GETs and POSTs
    @property
    def instance_locator(self) -> Optional[str]:
        """
        Construct API URL components to resolve requests in a multi-ODS

        :return: A URL component for use in the construction of requests.
        """
        if self.api_mode is None:
            return None

        elif self.api_mode in ('shared_instance', 'sandbox', 'district_specific',):
            return None

        elif self.api_mode in ('year_specific',):
            if not self.api_year:
                raise ValueError(
                    "`api_year` required for 'year_specific' mode."
                )
            return str(self.api_year)

        elif self.api_mode in ('instance_year_specific',):
            if not self.api_year or not self.instance_code:
                raise ValueError(
                    "`instance_code` and `api_year` required for 'instance_year_specific' mode."
                )
            return f"{self.instance_code}/{self.api_year}"

        else:
            raise ValueError(
                "`api_mode` must be one of: [shared_instance, sandbox, district_specific, year_specific, instance_year_specific].\n"
                "Use `get_api_mode()` to infer the api_mode of your instance."
            )

    @classmethod
    def is_edfi2(cls) -> bool:
        """
        EdFi2 functionality is removed after 0.2.4.
        """
        return False


    ### Methods for connecting to the ODS
    def connect(self,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 1200,
        use_snapshot: bool = False,
        **kwargs
    ) -> EdFiSession:
        return self.session.connect(
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            use_snapshot=use_snapshot, verify_ssl=self.verify_ssl, **kwargs
        )
    

    def get_token_info(self) -> dict:
        """
        The Ed-Fi API provides a way to get information about the education organization related to a token.
        https://edfi.atlassian.net/wiki/spaces/ODSAPIS3V520/pages/25100511/Authorization
        """
        token_info_url = util.url_join(self.base_url, "oauth/token_info")
        logging.info(f"[Get Token Info] Endpoint: {token_info_url}")

        token_response = self.session.post_response(
            token_info_url,
            data={'token': self.session.access_token},  # This attribute is defined on first authentication.
            remove_snapshot_header=True  # The token_info endpoint is incompatible with snapshots.
        )
        token_response.raise_for_status()
        return token_response.json()


    def get_newest_change_version(self) -> int:
        """
        Return the newest change version marked in the ODS (Ed-Fi3 only).

        :return:
        """
        change_version_url = util.url_join(self.base_url, 'changeQueries/v1', self.instance_locator, 'availableChangeVersions')
        logging.info(f"[Get Newest Change Version] Endpoint: {change_version_url}")

        res = self.session.get_response(change_version_url)
        if not res.ok:
            http_error_msg = (
                f"Change version check failed with status `{res.status_code}`: {res.reason}"
            )
            raise HTTPError(http_error_msg, response=res)

        # Ed-Fi 6.0 changes the key from `NewestChangeVersion` to `newestChangeVersion`.
        lower_json = {key.lower(): value for key, value in res.json().items()}
        return lower_json['newestchangeversion']


    def resource(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        get_deletes: bool = False,
        get_key_changes: bool = False,

        params: Optional[dict] = None,
        **kwargs
    ) -> EdFiResource:
        return EdFiResource(
            client=self,
            name=name, namespace=namespace, get_deletes=get_deletes, get_key_changes=get_key_changes,
            params=params, **kwargs
        )
    

    def descriptor(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        params: Optional[dict] = None,
        **kwargs
    ) -> EdFiDescriptor:
        """
        Even though descriptors and resources are accessed via the same endpoint,
        this may not be known to users, so a separate method is defined.
        """
        return EdFiDescriptor(
            client=self,
            name=name, namespace=namespace, get_deletes=False, get_key_changes=False,
            params=params, **kwargs
        )


    def composite(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        params: Optional[dict] = None,
        composite: str = 'enrollment',
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,
        **kwargs
    ) -> EdFiComposite:
        return EdFiComposite(
            client=self,
            name=name, namespace=namespace, composite=composite,
            filter_type=filter_type, filter_id=filter_id,
            params=params, **kwargs
        )
