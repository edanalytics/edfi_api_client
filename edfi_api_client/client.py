import functools
import requests
from requests.exceptions import HTTPError

from edfi_api_client import util
from edfi_api_client.async_mixin import AsyncEdFiSession
from edfi_api_client.endpoint import EdFiResource, EdFiDescriptor, EdFiComposite
from edfi_api_client.session import EdFiSession
from edfi_api_client.swagger import EdFiSwagger

from typing import Callable, List, Optional


import logging
logging.basicConfig(
    level="WARNING",
    format='[%(asctime)s] %(levelname)-8s: %(message)s',
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
        api_mode     : Optional[str] = None,
        api_year     : Optional[int] = None,
        instance_code: Optional[str] = None,
        api_version  : int = 3,  # Deprecated

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
        self.resources_swagger: EdFiSwagger = self.get_swagger('resources')
        self.descriptors_swagger: EdFiSwagger = self.get_swagger('descriptors')
        self.composites_swagger: EdFiSwagger = self.get_swagger('composites')

        # If ID and secret are passed, prepare synchronous and asynchronous sessions.
        self.session: Optional[EdFiSession] = None
        self.async_session: Optional[AsyncEdFiSession] = None

        if self.client_key and self.client_secret:
            # Synchronous client connects immediately on init.
            self.session = EdFiSession(self.base_url, self.client_key, self.client_secret, verify_ssl=verify_ssl)
            self.session.connect()
            logging.info("Connection to ODS successful!")

            # Asynchronous client connects only when called in an async method.
            self.async_session = AsyncEdFiSession(self.base_url, self.client_key, self.client_secret, verify_ssl=verify_ssl)

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


    ### Methods using the base URL info
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

        This method is lazy to circumvent multiple API calls to the same endpoint.

        :return: The descriptive payload returned by the API host.
        """
        return requests.get(self.base_url, verify=self.verify_ssl).json()

    @property
    def info(self) -> dict:
        if self._info is None:
            self._info = self.get_info()
        return self._info


    # API Mode
    # TODO: Make GETs eager and properties lazy.
    def get_api_mode(self) -> Optional[str]:
        """
        Retrieve api_mode from the metadata exposed at the API root.
        :return:
        """
        api_mode = self.info.get('apiMode')
        return util.camel_to_snake(api_mode) if api_mode else None


    # ODS Version
    def get_ods_version(self) -> Optional[str]:
        """
        Retrieve ods_version from the metadata exposed at the API root.
        :return:
        """
        return self.info.get('version')

    @property
    def ods_version(self) -> Optional[str]:
        return self.get_ods_version()


    # Data Model Version
    def get_data_model_version(self) -> Optional[str]:
        """
        Retrieve Ed-Fi data model version from the metadata exposed at the API root.
        :return:
        """
        data_models = self.info.get('dataModels', [])

        for data_model_dict in data_models:
            if data_model_dict.get('name') == 'Ed-Fi':
                return data_model_dict.get('version')
        else:
            return None

    @property
    def data_model_version(self) -> Optional[str]:
        return self.get_data_model_version()


    # Instance Locator
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
                raise ValueError("`api_year` required for 'year_specific' mode.")
            return str(self.api_year)

        elif self.api_mode in ('instance_year_specific',):
            if not self.api_year or not self.instance_code:
                raise ValueError("`instance_code` and `api_year` required for 'instance_year_specific' mode.")
            return f"{self.instance_code}/{self.api_year}"

        else:
            raise ValueError(
                "`api_mode` must be one of: [shared_instance, sandbox, district_specific, year_specific, instance_year_specific].\n"
                "Use `get_api_mode()` to infer the api_mode of your instance."
            )

    @property
    def instance_locator(self) -> Optional[str]:
        return self.get_instance_locator()


    # URLs  TODO: Should these be pulled self.info?
    @property
    def oauth_url(self) -> str:
        return util.url_join(self.base_url, 'oauth/token')

    @property
    def resource_url(self) -> str:
        return util.url_join(self.base_url, 'data/v3', self.instance_locator)

    @property
    def composite_url(self) -> str:
        return util.url_join(self.base_url, 'composites/v1', self.instance_locator)


    ### Methods for accessing ODS endpoints
    def _require_session(func: Callable) -> Callable:
        """
        This decorator verifies a session is established before calling the associated class method.

        :param func:
        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            if self.session is None:
                raise ValueError(
                    "An established connection to the ODS is required! Provide the client_key and client_secret in EdFiClient arguments."
                )
            return func(self, *args, **kwargs)
        return wrapped

    # TODO: ALL requests go through Session!
    @_require_session
    def get_newest_change_version(self) -> int:
        """
        Return the newest change version marked in the ODS (Ed-Fi3 only).

        :return:
        """
        change_query_path = util.url_join(
            self.base_url, 'changeQueries/v1', self.instance_locator, 'availableChangeVersions'
        )

        res = self.session.get_response(change_query_path)
        if not res.ok:
            http_error_msg = (
                f"Change version check failed with status `{res.status_code}`: {res.reason}"
            )
            raise HTTPError(http_error_msg, response=res)

        # Ed-Fi 6.0 changes the key from `NewestChangeVersion` to `newestChangeVersion`.
        lower_json = {key.lower(): value for key, value in res.json().items()}
        return lower_json['newestchangeversion']


    ### Endpoint Initializers
    @_require_session
    def resource(self,
        name: str,
        *,
        namespace: str = 'ed-fi',
        params: Optional[dict] = None,
        get_deletes: bool = False,
        **kwargs
    ) -> EdFiResource:
        return EdFiResource(
            self.resource_url, name, namespace=namespace, get_deletes=get_deletes, params=params,
            session = self.session, swagger=self.resources_swagger,
            **kwargs
        )

    @_require_session
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
            session=self.session, swagger=self.descriptors_swagger,
            **kwargs
        )

    @_require_session
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
            self.composite_url, name, namespace=namespace, params=params,
            composite=composite, filter_type=filter_type, filter_id=filter_id,
            session=self.session, swagger=self.composites_swagger,
            **kwargs
        )


    ### Methods related to retrieving the Swagger or attributes retrieved therein
    def get_swagger(self, component: str = 'resources'):
        return EdFiSwagger(self.base_url, component=component)

    @property
    def resources(self) -> List[str]:
        return self.resources_swagger.get_endpoints()

    @property
    def descriptors(self) -> List[str]:
        return self.descriptors_swagger.get_endpoints()
