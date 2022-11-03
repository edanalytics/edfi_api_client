import requests
import time

from functools import wraps
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from typing import Callable, Optional

from edfi_api_client import util
from edfi_api_client.edfi_endpoint import EdFiResource, EdFiComposite


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
    def __new__(cls, *args, **kwargs):
        """
        The user should never need to reference an `EdFi2Client` directly.
        This override of dunder-new makes the choice depending on passed `api_version`.

        :param args:
        :param kwargs:
        """
        api_version = kwargs.get('api_version', 3)

        if int(api_version) == 2:
            return object.__new__(EdFi2Client)
        else:
            return object.__new__(EdFiClient)


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
        self.verify_ssl = verify_ssl
        self.verbose = verbose

        self.base_url = base_url
        self.client_key = client_key
        self.client_secret = client_secret

        self.api_version = int(api_version)
        self.api_mode = api_mode or self.get_api_mode()
        self.api_year = api_year
        self.instance_code = instance_code

        # Build endpoint URL pieces
        self.version_url_string = self._get_version_url_string()
        self.instance_locator = self.get_instance_locator()

        # If ID and secret are passed, build a session.
        self.session = None

        if self.client_key and self.client_secret:
            self.connect()
        else:
            self.verbose_log("Client key and secret not provided. Connection with ODS will not be attempted.")


    def __repr__(self):
        """
        (Un)Authenticated Ed-Fi(2/3) Client [{api_mode}]
        """
        _session_string = "Authenticated" if self.session else "Unauthenticated"
        _api_mode = util.snake_to_camel(self.api_mode)
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


    def get_swagger(self, component: str = 'resources') -> dict:
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
        return requests.get(swagger_url, verify=self.verify_ssl).json()


    ### Helper methods for building elements of endpoint URLs for GETs and POSTs
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


    def _get_version_url_string(self) -> str:
        return "data/v3"


    ### Methods for logging and versioning
    def is_edfi2(self) -> bool:
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
            print(message)


    ### Methods for connecting to the ODS
    def connect(self) -> requests.Session:
        """
        Create a session with authorization headers.

        :return:
        """
        token_path = 'oauth/token'

        access_response = requests.post(
            util.url_join(self.base_url, token_path),
            auth=HTTPBasicAuth(self.client_key, self.client_secret),
            data={'grant_type': 'client_credentials'},
            verify=self.verify_ssl
        )
        access_response.raise_for_status()

        access_token = access_response.json().get('access_token')
        req_header = {'Authorization': 'Bearer {}'.format(access_token)}

        # Create a session and add headers to it.
        self.session = requests.Session()
        self.session.headers.update(req_header)

        # Add new attribute to track when connection was established.
        self.session.timestamp_unix = int(time.time())
        self.session.verify = self.verify_ssl

        self.verbose_log("Connection to ODS successful!")
        return self.session


    def require_session(func: Callable) -> Callable:
        """
        This decorator verifies a session is established before calling the associated class method.

        :param func:
        :return:
        """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            if self.session is None:
                raise ValueError(
                    "An established connection to the ODS is required! Provide the client_key and client_secret in EdFiClient arguments."
                )
            return func(self, *args, **kwargs)
        return wrapped


    ### Methods for accessing ODS endpoints
    @require_session
    def get_newest_change_version(self) -> int:
        """
        Return the newest change version marked in the ODS (Ed-Fi3 only).

        :return:
        """
        change_query_path = util.url_join(
            self.base_url, 'changeQueries/v1', self.instance_locator, 'availableChangeVersions'
        )

        res = self.session.get(change_query_path)
        if not res.ok:
            http_error_msg = (
                f"Change version check failed with status `{res.status_code}`: {res.reason}"
            )
            raise HTTPError(http_error_msg, response=res)

        return res.json()['NewestChangeVersion']


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
    ) -> EdFiResource:
        """
        Even though descriptors and resources are accessed via the same endpoint,
        this may not be known to users, so a separate method is defined.
        """
        return self.resource(
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



class EdFi2Client(EdFiClient):
    """

    """
    ### Methods for accessing the Base URL payload and Swagger
    def get_info(self) -> dict:
        raise NotImplementedError(
            "Information endpoint not implemented in Ed-Fi 2."
        )

    def get_api_mode(self) -> str:
        raise NotImplementedError(
            "API mode cannot be inferred in Ed-Fi 2. Please specify using `api_mode`."
        )

    def get_ods_version(self) -> str:
        raise NotImplementedError(
            "ODS version cannot be inferred in Ed-Fi 2."
        )

    def get_data_model_version(self) -> str:
        raise NotImplementedError(
            "Data model version cannot be inferred in Ed-Fi 2."
        )

    def get_swagger(self, component: str = 'resources') -> dict:
        raise NotImplementedError(
            "Swagger specification not implemented in Ed-Fi 2."
        )


    ### Helper methods for building elements of endpoint URLs for GETs and POSTs
    def _get_version_url_string(self) -> str:
        return "api/v2.0"


    ### Methods for logging and versioning
    def is_edfi2(self) -> bool:
        return True


    ### Methods for connecting to the ODS
    def connect(self) -> requests.Session:
        """
        Create a session with authorization headers.

        :return:
        """
        login_path = 'oauth/authorize'
        token_path = 'oauth/token'

        json_header = {'Content-Type': 'application/json'}
        login_data = {
            'Client_id': self.client_key,
            'Response_type': 'code',
        }

        response_login = requests.post(
            util.url_join(self.base_url, login_path),
            data=login_data,
            verify=self.verify_ssl
        )
        response_login.raise_for_status()

        login_code = response_login.json().get('code')

        token_data = {
            'Client_id': self.client_key,
            'Client_secret': self.client_secret,
            'Code': login_code,
            'Grant_type': 'authorization_code'
        }
        access_response = requests.post(
            util.url_join(self.base_url, token_path),
            json=token_data,
            headers=json_header,
            verify=self.verify_ssl
        )
        access_response.raise_for_status()

        access_token = access_response.json().get('access_token')
        req_header = {'Authorization': 'Bearer {}'.format(access_token)}

        # Create a session and add headers to it.
        self.session = requests.Session()
        self.session.headers.update(req_header)
        self.session.headers.update(json_header)

        # Add new attribute to track when connection was established.
        self.session.timestamp_unix = int(time.time())
        self.session.verify = self.verify_ssl

        self.verbose_log("Connection to ODS successful!")
        return self.session


    ### Methods for accessing ODS endpoints
    def get_newest_change_version(self) -> int:
        raise NotImplementedError(
            "Change versions not implemented in Ed-Fi 2!"
        )
