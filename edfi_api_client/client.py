import importlib
import requests
from requests.exceptions import HTTPError

from edfi_api_client import util
from edfi_api_client.async_mixin import AsyncEdFiSession
from edfi_api_client.endpoint import EdFiResource, EdFiDescriptor, EdFiComposite
from edfi_api_client.session import EdFiSession
from edfi_api_client.swagger import EdFiSwagger

from typing import List, Optional


import logging
logging.basicConfig(
    level="WARNING",
    format='[%(asctime)s] %(levelname)s: %(message)s',  # format='[%(asctime)s] %(levelname)-8s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class EdFiClient:
    """
    Client for interacting with the Ed-Fi API.
    Includes methods for authentication, ODS info, Swagger parsing, and endpoint initialization.

    :param base_url: The root URL of the API, without components like `data/v3`
    :param client_key: Authentication key
    :param client_secret: Authentication secret
    :param api_version: 3 for Suite 3, 2 for older 2.x instances (functionality removed)
    :param api_mode: ['shared_instance', 'sandbox', 'district_specific', 'year_specific', 'instance_year_specific']
    :param api_year: Required only for 'year_specific' or 'instance_year_specific' modes
    :param instance_code: Only required for 'instance_specific' or 'instance_year_specific modes'
    """
    is_edfi2: bool = False  # Deprecated method

    def __init__(self,
        base_url     : str,
        client_key   : Optional[str] = None,
        client_secret: Optional[str] = None,

        *,
        api_version  : int = 3,  # Deprecated
        api_mode     : Optional[str] = None,
        api_year     : Optional[int] = None,
        instance_code: Optional[str] = None,

        verify_ssl   : bool = True,
        verbose      : bool = False,
    ):
        # Update logger first
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        self.base_url: str = base_url
        self.client_key: Optional[str] = client_key
        self.client_secret: Optional[str] = client_secret
        self.verify_ssl: bool = verify_ssl

        # Information from base URL get (retrieved lazily)
        self._info: Optional[dict] = None

        self.api_version: int = int(api_version)
        self.api_mode: str = api_mode or self.get_api_mode()  # Populates self._info to infer mode from ODS.
        self.api_year: Optional[int] = api_year
        self.instance_code: Optional[str] = instance_code

        # Swagger variables for populating resource metadata (retrieved lazily)
        self.resources_swagger  : EdFiSwagger = EdFiSwagger(self.base_url, 'resources')
        self.descriptors_swagger: EdFiSwagger = EdFiSwagger(self.base_url, 'descriptors')
        self.composites_swagger : EdFiSwagger = EdFiSwagger(self.base_url, 'composites')

        # If ID and secret are passed, prepare synchronous and asynchronous sessions.
        self.session: Optional[EdFiSession] = None
        self.async_session: Optional[AsyncEdFiSession] = None

        if self.client_key and self.client_secret:
            # Synchronous client connects immediately; async client connects only when called in an async method.
            self.session = EdFiSession(self.oauth_url, self.client_key, self.client_secret, verify_ssl=verify_ssl).connect()
            self.async_session = AsyncEdFiSession(self.oauth_url, self.client_key, self.client_secret, verify_ssl=verify_ssl)
            logging.info("Connection to ODS successful!")
        else:
            logging.info("Client key and secret not provided. Connection with ODS will not be attempted.")

    def __repr__(self) -> str:
        """
        (Un)Authenticated Ed-Fi3 Client [{api_mode}]
        """
        session_string = "Authenticated" if self.session else "Unauthenticated"
        api_mode = util.snake_to_camel(self.api_mode) if self.api_mode else "None"
        if self.api_year:
            api_mode += f" {self.api_year}"

        return f"<{session_string} Ed-Fi{self.api_version} API Client [{api_mode}]>"


    ### Unauthenticated base-URL payload methods
    def get_info(self) -> dict:
        """
        Ed-Fi3 returns a helpful payload from the base URL.

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
        if self._info is None:
            self._info = requests.get(self.base_url, verify=self.verify_ssl).json()
        return self._info

    def get_api_mode(self) -> Optional[str]:
        """
        Retrieve api_mode from the metadata exposed at the API root.
        After API mode is deprecated in Ed-Fi 7, we can consider deprecating this method.
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

    def get_instance_locator(self) -> Optional[str]:
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
                logging.critical("`api_year` required for 'year_specific' mode.")
                exit(1)
            return str(self.api_year)

        elif self.api_mode in ('instance_year_specific',):
            if not self.api_year or not self.instance_code:
                logging.critical("`instance_code` and `api_year` required for 'instance_year_specific' mode.")
                exit(1)
            return f"{self.instance_code}/{self.api_year}"

        else:
            logging.critical(
                "`api_mode` must be one of: [shared_instance, sandbox, district_specific, year_specific, instance_year_specific].\n"
                "Use `get_api_mode()` to infer the api_mode of your instance."
            )
            exit(1)


    # URLs
    # TODO: Should these be built here, or pulled from `self._info`?
    @property
    def oauth_url(self) -> str:
        return util.url_join(self.base_url, 'oauth/token')

    @property
    def resource_url(self) -> str:
        return util.url_join(self.base_url, 'data/v3', self.get_instance_locator())

    @property
    def composite_url(self) -> str:
        return util.url_join(self.base_url, 'composites/v1', self.get_instance_locator())

    @property
    def change_version_url(self) -> str:
        return util.url_join(self.base_url, 'changeQueries/v1', self.get_instance_locator(), 'availableChangeVersions')


    ### Unauthenticated Swagger methods
    def get_swagger(self, component: str = 'resources'):
        swagger = EdFiSwagger(self.base_url, component=component)
        _ = swagger.payload  # Force eager execution
        return swagger

    @property
    def resources(self) -> List[str]:
        return self.resources_swagger.get_endpoints()

    @property
    def descriptors(self) -> List[str]:
        return self.descriptors_swagger.get_endpoints()


    ### Methods for accessing ODS endpoints
    def _require_session(self):
        if self.session is None:
            logging.critical(
                "An established connection to the ODS is required! Provide the client_key and client_secret in EdFiClient arguments."
            )

    def get_newest_change_version(self) -> int:
        """
        Return the newest change version marked in the ODS (Ed-Fi3 only).

        :return:
        """
        self._require_session()

        res = self.session.get_response(self.change_version_url)
        if not res.ok:
            http_error_msg = (
                f"Change version check failed with status `{res.status_code}`: {res.reason}"
            )
            raise HTTPError(http_error_msg, response=res)

        # Ed-Fi 6.0 changes the key from `NewestChangeVersion` to `newestChangeVersion`.
        lower_json = {key.lower(): value for key, value in res.json().items()}
        return lower_json['newestchangeversion']


    ### Endpoint Initializers
    def resource(self,
        name: str,
        *,
        namespace: str = 'ed-fi',
        params: Optional[dict] = None,
        get_deletes: bool = False,
        **kwargs
    ) -> EdFiResource:
        """

        """
        return EdFiResource(
            self.resource_url, name, namespace=namespace, get_deletes=get_deletes, params=params,
            session = self.session, async_session=self.async_session, swagger=self.resources_swagger,
            **kwargs
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
            self.resource_url, name, namespace=namespace, params=params,
            session=self.session, async_session=self.async_session, swagger=self.descriptors_swagger,
            **kwargs
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
        """

        """
        return EdFiComposite(
            self.composite_url, name, namespace=namespace, params=params,
            composite=composite, filter_type=filter_type, filter_id=filter_id,
            session=self.session, async_session=self.async_session, swagger=self.composites_swagger,
            **kwargs
        )
