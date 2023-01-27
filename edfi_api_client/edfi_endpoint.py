import abc
import logging
import requests
import time

from requests.exceptions import HTTPError, RequestsWarning
from typing import Iterator, List, Optional

from edfi_api_client.edfi_params import EdFiParams
from edfi_api_client import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.edfi_client import EdFiClient


class EdFiEndpoint:
    """

    """
    client: 'EdFiClient'
    name: str
    namespace: str

    url: str
    params: EdFiParams


    @abc.abstractmethod
    def build_url(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        **kwargs
    ):
        """
        This method builds the endpoint URL with namespacing and optional pathing.

        :param name:
        :param namespace:
        :param kwargs:
        :return:
        """
        raise NotImplementedError


    def ping(self) -> requests.Response:
        """
        This method pings the endpoint and verifies it is accessible.

        :return:
        """
        params = self.params.copy()
        params['limit'] = 1

        res = self.client.session.get(self.url, params=params)

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

        **kwargs
    ) -> Iterator[dict]:
        """
        This method returns all rows from an endpoint, applying pagination logic as necessary.
        Rows are returned as a generator.

        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :return:
        """
        paged_result_iter = self.get_pages(
            page_size=page_size,
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            **kwargs
        )

        for paged_result in paged_result_iter:
            for row in paged_result:
                yield row


    @abc.abstractmethod
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
        :param kwargs:
        :return:
        """
        raise NotImplementedError


    @abc.abstractmethod
    def total_count(self) -> int:
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        raise NotImplementedError


    ### Internal GET response methods and error-handling
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
        response = None

        for n_tries in range(max_retries):

            try:
                response = self.client.session.get(url, params=params)
                self.custom_raise_for_status(response)
                return response

            except RequestsWarning:
                # If an API call fails, it may be due to rate-limiting.
                # Use exponential backoff to wait, then refresh and try again.
                time.sleep(
                    min((2 ** n_tries) * 2, max_wait)
                )

                # Tokens have expiry times; refresh the token if it expires mid-run.
                authentication_delta = int(time.time()) - self.client.session.timestamp_unix

                self.client.verbose_log(
                    f"Maybe the session needs re-authentication? ({util.seconds_to_text(authentication_delta)} since last authentication)\n"
                    "Attempting reconnection..."
                )
                self.client.connect()

                logging.warning(f"Retry number: {n_tries}")

        # This block is reached only if max_retries has been reached.
        else:
            logging.error("API GET failed: max retries exceeded for URL.")

            self.client.verbose_log(message=(
                f"[Get with Retry Failed] Endpoint  : {url}\n"
                f"[Get with Retry Failed] Parameters: {params}"
            ), verbose=True)

            self.custom_raise_for_status(response)


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
                raise RequestsWarning(
                    "400: Bad request. Check your params. Is 'limit' set too high? Does the connection need to be reset?"
                )
            elif response.status_code == 401:
                raise RequestsWarning(
                    "401: Unauthorized for URL. The connection may need to be reset."
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

    """
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
        self.name: str = util.snake_to_camel(name)
        self.namespace: str = namespace
        self.get_deletes: bool = get_deletes

        self.url = self.build_url(self.name, namespace=self.namespace, get_deletes=self.get_deletes)
        self.params = EdFiParams(params, **kwargs)


    def __repr__(self):
        """
        Resource (Deletes)                     [{namespace}/{name}]
                           with {N} parameters
        """
        _deletes_string = " Deletes" if self.get_deletes else ""
        _params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        _full_name = f"{util.snake_to_camel(self.namespace)}/{util.snake_to_camel(self.name)}"

        return f"<Resource{_deletes_string}{_params_string} [{_full_name}]>"


    def build_url(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        get_deletes: bool = False
    ) -> str:
        """
        Build the name/descriptor URL to GET from the API.

        :param name:
        :param namespace:
        :param get_deletes:
        :return:
        """
        # Namespaces are not implemented in EdFi 2.x.
        if self.client.is_edfi2():
            namespace = None

        # Deletes are an optional path addition.
        deletes = None
        if get_deletes:
            deletes = 'deletes'

        return util.url_join(
            self.client.base_url,
            self.client.version_url_string,
            self.client.instance_locator,
            namespace, name, deletes
        )


    def get(self, limit: Optional[int] = None):
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        self.client.verbose_log(
            f"[Get Resource] Endpoint  : {self.url}\n"
            f"[Get Resource] Parameters: {self.params}"
        )
        return super().get(limit)


    def get_pages(self,
        *,
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000
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
        :return:
        """
        # Reset pagination parameters
        paged_params = self.params.copy()
        paged_params.init_page_by_offset(page_size)

        if step_change_version:
            paged_params.init_page_by_change_version_step(change_version_step_size)

        # Begin pagination-loop
        self.client.verbose_log(f"[Paged Get Resource] Endpoint  : {self.url}")

        while True:
            self.client.verbose_log(f"[Paged Get Resource] Parameters: {paged_params}")

            if retry_on_failure:
                res = self._get_response_with_exponential_backoff(
                    self.url, params=paged_params,
                    max_retries=max_retries, max_wait=max_wait
                )
            else:
                res = self._get_response(self.url, params=paged_params)

            # If no rows are returned, end pagination.
            if len(res.json()) == 0:

                if step_change_version:
                    try:
                        self.client.verbose_log(f"[Paged Get Resource] @ Retrieved zero rows. Stepping change version...")
                        paged_params.page_by_change_version_step()
                        # This raises a StopIteration if max change version is exceeded.
                    except StopIteration:
                        self.client.verbose_log(f"[Paged Get Resource] @ Change version exceeded max. Ending pagination.")
                        break
                else:
                    self.client.verbose_log(f"[Paged Get Resource] @ Retrieved zero rows. Ending pagination.")
                    break

            # Otherwise, paginate offset.
            else:
                self.client.verbose_log(f"[Paged Get Resource] @ Retrieved {len(res.json())} rows. Paging offset...")
                yield res.json()
                paged_params.page_by_offset()


    def total_count(self):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        params = self.params.copy()
        params['totalCount'] = True
        params['limit'] = 0

        res = self._get_response(self.url, params=params)
        return int(res.headers.get('Total-Count'))




class EdFiComposite(EdFiEndpoint):
    """

    """
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
        self.client: 'EdFiClient' = client
        self.name: str = util.snake_to_camel(name)
        self.namespace: str = namespace
        self.composite: str = composite
        self.filter_type: Optional[str] = filter_type
        self.filter_id: Optional[str] = filter_id

        self.url = self.build_url(
            self.name, namespace=self.namespace, composite=self.composite,
            filter_type=self.filter_type, filter_id=self.filter_id
        )
        self.params = EdFiParams(params, **kwargs)


    def __repr__(self):
        """
        Enrollment Composite                     [{namespace}/{name}]
                             with {N} parameters                      (filtered on {filter_type})
        """
        _composite = self.composite.title()
        _params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        _full_name = f"{util.snake_to_camel(self.namespace)}/{util.snake_to_camel(self.name)}"
        _filter_string = f" (filtered on {self.filter_type})" if self.filter_type else ""

        return f"<{_composite} Composite{_params_string} [{_full_name}]{_filter_string}>"


    def build_url(self,
        name: str,

        *,
        namespace: str = 'ed-fi',
        composite: str = 'enrollment',

        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,
    ) -> str:
        """
        Build the composite URL to GET from the API.

        :param name:
        :param namespace:
        :param composite:
        :param filter_type:
        :param filter_id:
        :return:
        """
        # Namespaces are not implemented in EdFi 2.x.
        if self.client.is_edfi2():
            namespace = None

        # If a filter is applied, the URL changes to match the filter type.
        if filter_type is None and filter_id is None:
            return util.url_join(
                self.client.base_url, 'composites/v1', self.client.instance_locator, namespace, composite, name.title()
            )

        elif filter_type is not None and filter_id is not None:
            return util.url_join(
                self.client.base_url, 'composites/v1', self.client.instance_locator, namespace, composite,
                filter_type, filter_id, name
            )

        else:
            raise ValueError(
                "`filter_type` and `filter_id` must both be specified if a filter is being applied!"
            )


    def get(self, limit: Optional[int] = None):
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        self.client.verbose_log(
            f"[Get Composite] Endpoint  : {self.url}\n"
            f"[Get Composite] Parameters: {self.params}"
        )
        return super().get(limit)


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
        if 'step_change_version' in kwargs or 'change_version_step_size' in kwargs:
            raise KeyError(
                "Change versions are not implemented in composites! Remove `step_change_version` and `change_version_step_size` from arguments."
            )

        # Reset pagination parameters
        paged_params = self.params.copy()
        paged_params.init_page_by_offset(page_size)

        # Begin pagination-loop
        self.client.verbose_log(f"[Paged Get Composite] Endpoint  : {self.url}")

        while True:
            self.client.verbose_log(f"[Paged Get Composite] Parameters: {paged_params}")

            if retry_on_failure:
                res = self._get_response_with_exponential_backoff(
                    self.url, params=paged_params,
                    max_retries=max_retries, max_wait=max_wait
                )
            else:
                res = self._get_response(self.url, params=paged_params)

            # If no rows are returned, end pagination.
            if len(res.json()) == 0:
                self.client.verbose_log(f"[Paged Get Composite] @ Retrieved zero rows. Ending pagination.")
                break

            # Otherwise, paginate offset.
            else:
                self.client.verbose_log(f"[Paged Get Composite] @ Retrieved {len(res.json())} rows. Paging offset...")
                yield res.json()
                paged_params.page_by_offset()


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
