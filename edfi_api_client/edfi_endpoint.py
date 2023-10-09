import logging
import requests
import time

from functools import wraps
from requests.exceptions import HTTPError, RequestsWarning
from typing import Callable, Iterator, List, Optional

from edfi_api_client.edfi_params import EdFiParams
from edfi_api_client import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.edfi_client import EdFiClient


class EdFiEndpoint:
    """
    This is an abstract class for interacting with Ed-Fi resources and descriptors.
    Composites override EdFiEndpoint with custom composite-logic.
    """
    type: str = None
    swagger_type: str = None

    def __init__(self,
         client: 'EdFiClient',
         name: str,

         *,
         namespace: str = 'ed-fi',
         get_deletes: bool = False,

         params: Optional[dict] = None,
         **kwargs
    ):
        self.client: 'EdFiClient' = client

        # Name and namespace can be passed manually
        if isinstance(name, str):
            self.name: str = util.snake_to_camel(name)
            self.namespace: str = namespace

        # Or as a `(namespace, name)` tuple as output from Swagger
        else:
            try:
                self.namespace, self.name = name
            except ValueError:
                logging.error(
                    "Arguments `name` and `namespace` must be passed explicitly, or as a `(namespace, name)` tuple."
                )

        # Build URL and dynamic params object
        self.get_deletes: bool = get_deletes
        self.url = self._build_endpoint_url()
        self.params = EdFiParams(params, **kwargs)

        # Swagger attributes are loaded lazily
        self.swagger = self.client.swaggers.get(self.swagger_type)

    def __repr__(self):
        """
        Endpoint (Deletes) (with {N} parameters) [{namespace}/{name}]
        """
        deletes_string = " Deletes" if self.get_deletes else ""
        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        full_name = f"{util.snake_to_camel(self.namespace)}/{util.snake_to_camel(self.name)}"

        return f"<{self.type}{deletes_string}{params_string} [{full_name}]>"

    def ping(self) -> requests.Response:
        """
        This method pings the endpoint and verifies it is accessible.

        :return:
        """
        params = self.params.copy()
        params['limit'] = 1

        res = self._get_response(self.url, params=params)

        # To ping a composite, a limit of at least one is required.
        # We do not want to surface student-level data during ODS-checks.
        if res.ok:
            res._content = b'{"message": "Ping was successful! ODS data has been intentionally scrubbed from this response."}'

        return res

    def get(self, limit: Optional[int] = None) -> List[dict]:
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        self.client.verbose_log(f"[Get {self.type}] Endpoint  : {self.url}")
        self.client.verbose_log(f"[Get {self.type}] Parameters: {self.params}")

        params = self.params.copy()

        if limit is not None:
            params['limit'] = limit

        return self._get_response(self.url, params=params).json()

    def get_rows(self,
        *,
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True
    ) -> Iterator[dict]:
        """
        This method returns all rows from an endpoint, applying pagination logic as necessary.
        Rows are returned as a generator.

        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        paged_result_iter = self.get_pages(
            page_size=page_size,
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
        )

        for paged_result in paged_result_iter:
            yield from paged_result

    def get_pages(self,
        *,
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True,
    ) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a generator.

        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        self.client.verbose_log(f"[Paged Get {self.type}] Endpoint  : {self.url}")

        # Build a list of pagination params to iterate during ingestion.
        if step_change_version:
            self.client.verbose_log(
                f"[Paged Get {self.type}] Pagination Method: Change Version Stepping{' with Reverse-Offset Pagination' if reverse_paging else ''}"
            )

            paged_params_list = []

            for cv_window_params in self.params.build_change_version_window_params(change_version_step_size):
                total_count = self._get_total_count(cv_window_params)
                cv_offset_params_list = cv_window_params.build_offset_window_params(page_size, total_count=total_count)

                if reverse_paging:
                    cv_offset_params_list = list(cv_offset_params_list)[::-1]

                paged_params_list.extend(cv_offset_params_list)

        else:
            self.client.verbose_log(
                f"[Paged Get {self.type}] Pagination Method: Offset Pagination"
            )

            total_count = self._get_total_count(self.params)
            paged_params_list = self.params.build_offset_window_params(page_size, total_count=total_count)

        # Begin pagination-loop
        for paged_params in paged_params_list:

            ### GET from the API and yield the resulting JSON payload
            self.client.verbose_log(f"[Paged Get {self.type}] Parameters: {paged_params}")

            if retry_on_failure:
                res = self._get_response_with_exponential_backoff(
                    self.url, params=paged_params,
                    max_retries=max_retries, max_wait=max_wait
                )
            else:
                res = self._get_response(self.url, params=paged_params)

            self.client.verbose_log(f"[Paged Get {self.type}] Retrieved {len(res.json())} rows.")
            yield res.json()

    def total_count(self):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        return self._get_total_count(self.params)


    ### Swagger-adjacent properties and helper methods
    @property
    def description(self) -> str:
        if self.swagger is None:
            self.swagger = self.client.get_swagger(self.swagger_type)
        return self.swagger.descriptions.get(self.name)

    @property
    def has_deletes(self) -> bool:
        if self.swagger is None:
            self.swagger = self.client.get_swagger(self.swagger_type)
        return (self.namespace, self.name) in self.swagger.deletes


    ### Internal helpers, GET response methods, and error-handling
    def _build_endpoint_url(self) -> str:
        """
        Build the name/descriptor URL to GET from the API.

        :return:
        """
        # Deletes are an optional path addition.
        deletes = 'deletes' if self.get_deletes else None

        return util.url_join(
            self.client.base_url,
            self.client.version_url_string,
            self.client.instance_locator,
            self.namespace, self.name, deletes
        )

    def reconnect_if_expired(func: Callable) -> Callable:
        """
        This decorator resets the connection with the API if expired.

        :param func:
        :return:
        """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            # Refresh token if refresh_time has passed
            if self.client.session.refresh_time < int(time.time()):
                self.client.verbose_log("Session authentication is expired. Attempting reconnection...")
                self.client.connect()

            return func(self, *args, **kwargs)
        return wrapped

    @reconnect_if_expired
    def _get_total_count(self, url: str, params: EdFiParams):
        """
        `total_count()` is accessible by the user and during reverse offset-pagination.
        This internal helper method prevents code needing to be defined twice.

        :param url:
        :param params:
        :return:
        """
        _params = params.copy()
        _params['totalCount'] = True
        _params['limit'] = 0

        res = self._get_response(url, params=_params)
        return int(res.headers.get('Total-Count'))

    @reconnect_if_expired
    def _get_response(self,
        url: str,
        params: Optional[EdFiParams] = None
    ) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        response = self.client.session.get(url, params=params)
        self.custom_raise_for_status(response)
        return response


    @reconnect_if_expired
    def _get_response_with_exponential_backoff(self,
        url: str,
        params: Optional[EdFiParams] = None,

        *,
        max_retries: int = 5,
        max_wait: int = 600,
    ) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.
        In the case of failure, retry with exponential backoff until max_retries or max_wait has been exceeded.

        :param url:
        :param params:
        :param max_retries:
        :param max_wait:
        :return:
        """
        # Attempt the GET until success or `max_retries` reached.
        for n_tries in range(max_retries):

            try:
                return self._get_response(url, params=params)

            except RequestsWarning:
                # If an API call fails, it may be due to rate-limiting.
                # Use exponential backoff to wait, then refresh and try again.
                time.sleep(
                    min((2 ** n_tries) * 2, max_wait)
                )
                logging.warning(f"Retry number: {n_tries}")

        # This block is reached only if max_retries has been reached.
        else:
            self.client.verbose_log(message=(
                f"[Get with Retry Failed] Endpoint  : {url}\n"
                f"[Get with Retry Failed] Parameters: {params}"
            ), verbose=True)

            raise RuntimeError(
                "API GET failed: max retries exceeded for URL."
            )

    @staticmethod
    def custom_raise_for_status(response):
        """
        Custom HTTP exception logic and logging.
        The built-in Response.raise_for_status() fails too broadly, even in cases where a connection-reset is enough.

        :param response:
        :return:
        """
        if 400 <= response.status_code < 600:
            logging.warning(
                f"API Error: {response.status_code} {response.reason}"
            )
            if response.status_code == 400:
                raise HTTPError(
                    "400: Bad request. Check your params. Is 'limit' set too high?"
                )
            elif response.status_code == 401:
                raise RequestsWarning(
                    "401: Unauthenticated for URL. The connection may need to be reset."
                )
            elif response.status_code == 403:
                # Only raise an HTTPError where the resource is impossible to access.
                raise HTTPError(
                    "403: Resource not authorized.",
                    response=response
                )
            elif response.status_code == 404:
                # Only raise an HTTPError where the resource is impossible to access.
                raise HTTPError(
                    "404: Resource not found.",
                    response=response
                )
            elif response.status_code == 500:
                raise RequestsWarning(
                    "500: Internal server error."
                )
            elif response.status_code == 504:
                raise RequestsWarning(
                    "504: Gateway time-out for URL. The connection may need to be reset."
                )
            else:
                # Otherwise, use the default error messages defined in Response.
                response.raise_for_status()


class EdFiResource(EdFiEndpoint):
    """
    Ed-Fi Resources are the primary use-case of the API.
    """
    type: str = 'Resource'
    swagger_type: str = 'resources'


class EdFiDescriptor(EdFiEndpoint):
    """
    Ed-Fi Descriptors are used identically to Resources, but they are listed in a separate Swagger.
    """
    type: str = 'Descriptor'
    swagger_type: str = 'descriptors'


class EdFiComposite(EdFiEndpoint):
    """

    """
    type: str = 'Composite'
    swagger_type: str = 'composites'

    def __init__(self,
        client: 'EdFiClient',
        name: str,

        *,
        namespace: str = 'ed-fi',
        composite: str = 'enrollment',
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,

        params: Optional[dict] = None,
        **kwargs
    ):
        # Assign composite-specific arguments that are used in `self._build_endpoint_url()`.
        self.composite: str = composite
        self.filter_type: Optional[str] = filter_type
        self.filter_id: Optional[str] = filter_id

        super().__init__(client=client, name=name, namespace=namespace, params=params)

    def __repr__(self):
        """
        Enrollment Composite                     [{namespace}/{name}]
                             with {N} parameters                      (filtered on {filter_type})
        """
        composite = self.composite.title()
        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        full_name = f"{util.snake_to_camel(self.namespace)}/{util.snake_to_camel(self.name)}"
        filter_string = f" (filtered on {self.filter_type})" if self.filter_type else ""

        return f"<{composite} Composite{params_string} [{full_name}]{filter_string}>"

    def total_count(self):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        raise NotImplementedError(
            "Total counts have not yet been implemented in Ed-Fi composites!"
        )

    def get_pages(self,
        *,
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        **kwargs
    ) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a generator.

        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :return:
        """
        if 'step_change_version' in kwargs or 'change_version_step_size' in kwargs or 'reverse_paging' in kwargs:
            raise KeyError(
                "Change versions are not implemented in composites!\n"
                "Remove `step_change_version`, `change_version_step_size`, and/or `reverse_paging` from arguments."
            )

        self.client.verbose_log(f"[Paged Get {self.type}] Endpoint  : {self.url}")
        self.client.verbose_log(f"[Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Reset pagination parameters
        paged_params = self.params.copy()
        paged_params['limit'] = page_size
        paged_params['offset'] = 0

        # Begin pagination-loop
        while True:

            ### GET from the API and yield the resulting JSON payload
            self.client.verbose_log(f"[Paged Get {self.type}] Parameters: {paged_params}")

            if retry_on_failure:
                res = self._get_response_with_exponential_backoff(
                    self.url, params=paged_params,
                    max_retries=max_retries, max_wait=max_wait
                )
            else:
                res = self._get_response(self.url, params=paged_params)

            # If rows have been returned, there may be more to ingest.
            if res.json():
                self.client.verbose_log(f"[Paged Get {self.type}] Retrieved {len(res.json())} rows.")
                yield res.json()

                self.client.verbose_log(f"@ Paginating offset...")
                paged_params['offset'] += page_size

            # If no rows are returned, end pagination.
            else:
                self.client.verbose_log(f"[Paged Get {self.type}] @ Retrieved zero rows. Ending pagination.")
                break


    def _build_endpoint_url(self) -> str:
        """
        Build the composite URL to GET from the API.

        :return:
        """
        # If a filter is applied, the URL changes to match the filter type.
        if self.filter_type is None and self.filter_id is None:
            return util.url_join(
                self.client.base_url, 'composites/v1',
                self.client.instance_locator,
                self.namespace, self.composite, self.name.title()
            )

        elif self.filter_type is not None and self.filter_id is not None:
            return util.url_join(
                self.client.base_url, 'composites/v1',
                self.client.instance_locator,
                self.namespace, self.composite,
                self.filter_type, self.filter_id, self.name
            )

        else:
            raise ValueError(
                "`filter_type` and `filter_id` must both be specified if a filter is being applied!"
            )
