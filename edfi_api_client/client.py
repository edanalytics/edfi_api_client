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
    level="INFO",
    format='[%(asctime)s] %(levelname)-8s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class EdFiClient:
    """
    Client for interacting with the Ed-Fi API.
    Includes methods for authentication, parsing the Swagger, and getting content from endpoints.

    :param base_url: The root URL of the API, without components like `data/v3`
    :param client_key: Authentication key
    :param client_secret: Authentication secret
    :param api_version: 3 for Suite 3, 2 for older 2.x instances (functionality removed)
    :param api_mode: ['shared_instance', 'sandbox', 'district_specific', 'year_specific', 'instance_year_specific']
    :param api_year: Required only for 'year_specific' or 'instance_year_specific' modes
    :param instance_code: Only required for 'instance_specific' or 'instance_year_specific modes'
    """
    version_url_string: str = "data/v3"

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
        self.verbose = verbose

        self.base_url = base_url
        self.client_key = client_key
        self.client_secret = client_secret
        self.verify_ssl = verify_ssl

        self.api_version = int(api_version)
        self.api_mode = api_mode or self.get_api_mode()
        self.api_year = api_year
        self.instance_code = instance_code

        # Build endpoint URL pieces
        self.instance_locator = self.get_instance_locator()

        # Swagger variables for populating resource metadata (retrieved lazily)
        self.swaggers = {
            'resources'  : None,
            'descriptors': None,
            'composites' : None,
        }

        # If ID and secret are passed, build a session.
        self.session: EdFiSession = EdFiSession(self.base_url, self.client_key, self.client_secret, verify_ssl=verify_ssl)
        self.async_session: AsyncEdFiSession = AsyncEdFiSession(self.base_url, self.client_key, self.client_secret, verify_ssl=verify_ssl)

        if self.client_key and self.client_secret:
            self.session.connect()  # Connect synchronous session immediately
        else:
            self.verbose_log("Client key and secret not provided. Connection with ODS will not be attempted.")

    def __repr__(self) -> str:
        """
        (Un)Authenticated Ed-Fi3 Client [{api_mode}]
        """
        session_string = "Authenticated" if self.session else "Unauthenticated"
        api_mode = util.snake_to_camel(self.api_mode)
        if self.api_year:
            api_mode += f" {self.api_year}"

        return f"<{session_string} Ed-Fi{self.api_version} API Client [{api_mode}]>"

    @staticmethod
    def is_edfi2() -> bool:
        return False

    def verbose_log(self, message: str, verbose: bool = False):
        """
        Unified method for logging class state during API pulls.
        Set `self.verbose=True or verbose=True` to log.

        :param message:
        :param verbose:
        :return:
        """
        if self.verbose or verbose:
            logging.info(message)


    ### Methods for accessing the Base URL payload and Swagger
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
        return requests.get(self.base_url, verify=self.verify_ssl).json()

    def get_api_mode(self) -> str:
        """
        Retrieve api_mode from the metadata exposed at the API root.
        :return:
        """
        api_mode = self.get_info().get('apiMode')
        return util.camel_to_snake(api_mode)

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

    ### Methods for accessing ODS endpoints
    def require_session(func: Callable) -> Callable:
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

    @require_session
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

    @require_session
    def resource(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        get_deletes: bool = False,

        params: Optional[dict] = None,
        **kwargs
    ) -> EdFiResource:
        return EdFiResource(
            client=self,
            name=name, namespace=namespace, get_deletes=get_deletes,
            params=params, **kwargs
        )

    @require_session
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
            name=name, namespace=namespace, get_deletes=False,
            params=params, **kwargs
        )

    @require_session
    def composite(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        composite: str = 'enrollment',
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,

        params: Optional[dict] = None,
        **kwargs
    ) -> EdFiComposite:
        return EdFiComposite(
            client=self,
            name=name, namespace=namespace, composite=composite,
            filter_type=filter_type, filter_id=filter_id,
            params=params, **kwargs
        )


    ### Methods related to retrieving the Swagger or attributes retrieved therein
    def get_swagger(self, component: str = 'resources') -> EdFiSwagger:
        """
        OpenAPI Specification describes the entire Ed-Fi API surface in a
        JSON payload.
        Can be used to surface available endpoints.

        :param component: Which component's swagger spec should be retrieved?
        :return: Swagger specification definition, as a dictionary.
        """
        self.verbose_log(f"[Get {component.title()} Swagger] Retrieving Swagger into memory...")

        swagger_url = util.url_join(
            self.base_url, 'metadata', self.version_url_string, component, 'swagger.json'
        )

        payload = requests.get(swagger_url, verify=self.verify_ssl).json()
        swagger = EdFiSwagger(component, payload)

        # Save the swagger in memory to save time on subsequent calls.
        self.swaggers[component] = swagger
        return swagger

    @property
    def resources(self) -> List[str]:
        """
        Return a list of resource endpoints, as defined in Swagger.
        :return:
        """
        if self.swaggers['resources'] is None:
            self.get_swagger('resources')
        return self.swaggers['resources'].endpoints

    @property
    def descriptors(self) -> List[str]:
        """
        Return a list of descriptor endpoints, as defined in Swagger.
        :return:
        """
        if self.swaggers['descriptors'] is None:
            self.get_swagger('descriptors')
        return self.swaggers['descriptors'].endpoints
