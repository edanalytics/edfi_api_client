import logging
import requests
from typing import Optional

from edfi_api_client.edfi_session import EdFiSession, AsyncEdFiSession
from edfi_api_client import util
from edfi_api_client.edfi_endpoint import EdFiResource, EdFiDescriptor, EdFiComposite
from edfi_api_client.edfi_swagger import EdFiSwagger

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

        use_async    : bool = False,
        verify_ssl   : bool = True,
        verbose      : bool = False,
    ):
        logging.basicConfig(level="INFO")
        if verbose:
            logging.getLogger().setLevel("DEBUG")

        self.use_async = use_async
        self.verify_ssl = verify_ssl

        self.base_url = base_url
        self.client_key = client_key
        self.client_secret = client_secret

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

        # Build separate session objects, depending on synchronicity of API-use.
        if self.use_async:
            self.session = AsyncEdFiSession(
                base_url, client_key=client_key, client_secret=client_secret,
                verify_ssl=verify_ssl
            )
        else:
            self.session = EdFiSession(
                base_url, client_key=client_key, client_secret=client_secret,
                verify_ssl=verify_ssl
            )

    def __repr__(self):
        """
        (Un)Authenticated Ed-Fi3 Client [{api_mode}]
        """
        sync_string = "Asynchronous " if self.use_async else ""
        session_string = "Authenticated" if self.session else "Unauthenticated"
        api_mode = util.snake_to_camel(self.api_mode)
        if self.api_year:
            api_mode += f" {self.api_year}"

        return f"<{sync_string}{session_string} Ed-Fi{self.api_version} API Client [{api_mode}]>"

    @staticmethod
    def is_edfi2() -> bool:
        return False


    ### Methods for accessing the Base URL payload and Swagger
    def get_info(self) -> dict:
        """
        Ed-Fi3 returns a helpful payload from the base URL (no authentication required)

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
        res = requests.get(self.base_url, verify=self.verify_ssl)
        res.raise_for_status()
        return res.json()

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


    ### Helper methods for building elements of endpoint URLs for GETs and POSTs
    def get_instance_locator(self) -> Optional[str]:
        """
        Construct API URL components to resolve requests in a multi-ODS

        :return: A URL component for use in the construction of requests.
        """
        if not self.api_mode or self.api_mode in ('shared_instance', 'sandbox', 'district_specific',):
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
    def get_newest_change_version(self) -> int:
        """
        Return the newest change version marked in the ODS (Ed-Fi3 only).

        :return:
        """
        change_query_path = util.url_join(
            self.base_url, 'changeQueries/v1', self.instance_locator, 'availableChangeVersions'
        )

        res = self.session.get(change_query_path)

        # Ed-Fi 6.0 changes the key from `NewestChangeVersion` to `newestChangeVersion`.
        lower_json = {key.lower(): value for key, value in res.json().items()}
        return lower_json['newestchangeversion']

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
        OpenAPI Specification describes the entire Ed-Fi API surface in a JSON payload.
        Can be used to surface available endpoints.

        :param component: Which component's swagger spec should be retrieved?
        :return: Swagger specification definition, as a dictionary.
        """
        swagger_url = util.url_join(
            self.base_url, 'metadata', self.version_url_string, component, 'swagger.json'
        )

        logging.debug(f"[Get {component.title()} Swagger] Retrieving Swagger into memory...")
        res = requests.get(swagger_url, verify=self.verify_ssl)
        res.raise_for_status()
        swagger = EdFiSwagger(component, res.json())

        # Save the swagger in memory to save time on subsequent calls.
        self.swaggers[component] = swagger
        return swagger

    @property
    def resources(self):
        """

        :return:
        """
        swagger_type = 'resources'

        if self.swaggers[swagger_type] is None:
            self.get_swagger(swagger_type)

        return self.swaggers[swagger_type].endpoints

    @property
    def descriptors(self):
        """

        :return:
        """
        swagger_type = 'descriptors'

        if self.swaggers[swagger_type] is None:
            self.get_swagger(swagger_type)

        return self.swaggers[swagger_type].endpoints