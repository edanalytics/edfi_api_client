import asyncio
import aiohttp
import aiohttp_retry
import aiofiles
import functools
import json
import logging
import requests
import time

from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError

from edfi_api_client import util

from typing import Awaitable, AsyncIterator
from typing import Callable, Iterator, Optional, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.edfi_params import EdFiParams


class EdFiSession:
    """

    """
    retry_statuses: Tuple[int] = (401, 500, 504)
    session_class = requests.Session

    def __init__(self,
        base_url: str,
        client_key: Optional[str] = None,
        client_secret: Optional[str] = None,

        *,
        verify_ssl: bool = True,
        **kwargs
    ):
        self.base_url: str = base_url
        self.client_key: Optional[str] = client_key
        self.client_secret: Optional[str] = client_secret
        self.verify_ssl: bool = verify_ssl

        # If ID and secret are passed, build a session.
        self.session = None

        if self.client_key and self.client_secret:
            self.connect()
        else:
            logging.debug("Client key and secret not provided. Connection with ODS will not be attempted.")


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

        # Create a session using the specified class and add headers to it.
        self.session = self.session_class()
        self.session.headers.update(req_header)

        # Add new attributes to track when connection was established and when to refresh the access token.
        self.session.timestamp_unix = int(time.time())
        self.session.refresh_time = int(self.session.timestamp_unix + access_response.json().get('expires_in') - 120)
        self.session.verify = self.verify_ssl

        logging.debug("Connection to ODS successful!")
        return self.session


    ### Internal GET response methods and error-handling
    def reconnect_if_expired(func: Callable) -> Callable:
        """
        This decorator resets the connection with the API if expired.

        :param func:
        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            # Raise an error if the session is not authenticated.
            if not self.session:
                logging.critical(
                    "An established connection to the ODS is required! Provide the client_key and client_secret in EdFiClient arguments."
                )
                exit(1)

            # Refresh token if refresh_time has passed
            if self.session.refresh_time < int(time.time()):
                self.verbose_log(
                    "Session authentication is expired. Attempting reconnection..."
                )
                self.client.connect()
            return func(self, *args, **kwargs)

        return wrapped

    @reconnect_if_expired
    def get(self,
        url: str,
        params: Optional['EdFiParams'] = None,

        *,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 600,
        **kwargs
    ) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :return:
        """
        logging.debug(f"[GET] Parameters: {params}")

        if retry_on_failure:
            retry_strategy = Retry(
                total=max_retries,
                backoff_factor=2,
                status_forcelist=self.retry_statuses
            )
            retry_strategy.BACKOFF_MAX = max_wait

            retry_adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount(url, retry_adapter)

        response = self.session.get(url, params=params, verify=self.verify_ssl, **kwargs)
        self.custom_raise_for_status(response)
        return response

    @reconnect_if_expired
    def get_all(self, url: str, params_iter: Iterator['EdFiParams'], **kwargs):
        """
        Iterate multiple params across the same endpoint.
        :param url:
        :param params_iter:
        :param kwargs:
        :return:
        """
        for params in params_iter:
            yield self.get(url, params=params, **kwargs)

    @reconnect_if_expired
    def get_total_count(self, url: str, params: 'EdFiParams', **kwargs):
        """
        `total_count()` is accessible by the user and during pagination.
        This internal helper method prevents code needing to be defined twice.

        :param url:
        :param params:
        :return:
        """
        _params = params.copy()
        _params['totalCount'] = True
        _params['limit'] = 0

        res = self.get(url, params=_params, **kwargs)
        return int(res.headers.get('Total-Count'))

    @staticmethod
    def rows_to_disk(path: str, rows: Iterator[dict]) -> str:
        """

        :param path:
        :param rows:
        :return:
        """
        with open(path, 'wb') as fp:
            for row in rows:
                fp.write(json.dumps(row).encode('utf-8') + b'\n')

        return path

    @staticmethod
    def custom_raise_for_status(response):
        """
        Custom HTTP exception logic and logging.
        The built-in Response.raise_for_status() fails too broadly, even in cases where a connection-reset is enough.

        :param response:
        :return:
        """
        if not response.ok:
            logging.warning(f"API Error: {response.status_code} {response.reason}")

            if response.status_code == 400:
                error_message = "400: Bad request. Check your params. Is 'limit' set too high?"
            elif response.status_code == 401:
                error_message = "401: Unauthenticated for URL. The connection may need to be reset."
            elif response.status_code == 403:
                error_message = "403: Resource not authorized."
            elif response.status_code == 404:
                error_message = "404: Resource not found."
            elif response.status_code == 500:
                error_message = "500: Internal server error."
            elif response.status_code == 504:
                error_message = "504: Gateway time-out for URL. The connection may need to be reset."
            else:
                # Otherwise, use the default error messages defined in Response.
                response.raise_for_status()
                exit(1)

            raise HTTPError(error_message, response=response)


class AsyncEdFiSession(EdFiSession):
    """

    """
    session_class = aiohttp.ClientSession

    def build_retry_client(self, max_retries: int, max_wait: int, **kwargs) -> aiohttp_retry.RetryClient:
        """

        :param max_retries:
        :param max_wait:
        :return:
        """
        retry_options = aiohttp_retry.ExponentialRetry(
            attempts=max_retries,
            max_timeout=max_wait,
            statuses=set(self.retry_statuses)
        )

        return aiohttp_retry.RetryClient(
            client_session=self.session,
            retry_options=retry_options
        )

    @EdFiSession.reconnect_if_expired
    async def get(self,
        url: str,
        params: Optional['EdFiParams'] = None,

        *,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 600,

        client_session: Optional[aiohttp.ClientSession] = None,
        **kwargs
    ) -> aiohttp.ClientResponse:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param client_session:
        :return:
        """
        logging.debug(f"[GET] Parameters: {params}")

        if not client_session and retry_on_failure:
            client_session = self.build_retry_client(max_retries=max_retries, max_wait=max_wait)
        else:
            client_session = client_session or self.session

        async with client_session.get(url, params=params) as response:
            _ = await response.text()
            self.custom_raise_for_status(response)
            return response

    @EdFiSession.reconnect_if_expired
    async def get_all(self,
        url: str,
        params_iter: AsyncIterator['EdFiParams'],
        **kwargs
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """
        Iterate multiple params across the same endpoint.
        :param url:
        :param params_iter:
        :param kwargs:
        :return:
        """
        if 'retry_on_failure' in kwargs:
            client_session = self.build_retry_client(**kwargs)
        else:
            client_session = self.session

        async for params in params_iter:
            yield self.get(url, params=params, client_session=client_session, **kwargs)

    @EdFiSession.reconnect_if_expired
    async def get_total_count(self,
        url: str,
        params: 'EdFiParams',

        *,
        client_session: Optional[aiohttp_retry.ClientSession] = None
    ) -> int:
        """
        `total_count()` is accessible by the user and during pagination.
        This internal helper method prevents code needing to be defined twice.

        :param url:
        :param params:
        :param client_session:
        :return:
        """
        return super().get_total_count(url, params=params, client_session=client_session)

    @staticmethod
    async def rows_to_disk(path: str, rows: AsyncIterator[dict]) -> str:
        """

        :param path:
        :param rows:
        :return:
        """
        async with aiofiles.open(path, 'wb') as fp:
            async for row in rows:
                await fp.write(json.dumps(row).encode('utf-8') + b'\n')

        return path

# ## Async GET methods
# async def async_to_json(self,
#     path: str,
#
#     *,
#     page_size: int = 100,
#
#     retry_on_failure: bool = False,
#     max_retries: int = 5,
#     max_wait: int = 500,
#
#     step_change_version: bool = False,
#     change_version_step_size: int = 50000,
#     reverse_paging: bool = True
# ) -> Awaitable[str]:
#     """
#
#     :param path:
#     :param page_size:
#     :param retry_on_failure:
#     :param max_retries:
#     :param max_wait:
#     :param step_change_version:
#     :param change_version_step_size:
#     :param reverse_paging:
#     :return:
#     """
#     paged_results = self.async_get_pages(
#         page_size=page_size,
#         retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
#         step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
#     )
#
#     async with aiofiles.open(path, 'wb') as fp:
#         async for page in paged_results:
#             await fp.write(self.page_to_bytes(page))
#
#     return path
#
# async def async_get_rows(self,
#     *,
#     page_size: int = 100,
#
#     retry_on_failure: bool = False,
#     max_retries: int = 5,
#     max_wait: int = 500,
#
#     step_change_version: bool = False,
#     change_version_step_size: int = 50000,
#     reverse_paging: bool = True
# ) -> AsyncIterator[dict]:
#     """
#     This method returns all rows from an endpoint, applying pagination logic as necessary.
#     Rows are returned as a generator.
#
#     :param page_size:
#     :param retry_on_failure:
#     :param max_retries:
#     :param max_wait:
#     :param step_change_version:
#     :param change_version_step_size:
#     :param reverse_paging:
#     :return:
#     """
#     paged_result_iter = self.async_get_pages(
#         page_size=page_size,
#         retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
#         step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
#     )
#
#     async for paged_result in paged_result_iter:
#         for row in paged_result:
#             yield row
#
# async def async_get_pages(self,
#     *,
#     page_size: int = 100,
#
#     retry_on_failure: bool = False,
#     max_retries: int = 5,
#     max_wait: int = 500,
#
#     step_change_version: bool = False,
#     change_version_step_size: int = 50000,
#     reverse_paging: bool = True,
# ) -> AsyncIterator[List[dict]]:
#     """
#     This method completes a series of GET requests, paginating params as necessary based on endpoint.
#     Rows are returned as a generator.
#
#     :param page_size:
#     :param retry_on_failure:
#     :param max_retries:
#     :param max_wait:
#     :param step_change_version:
#     :param change_version_step_size:
#     :param reverse_paging:
#     :return:
#     """
#     self.client.verbose_log(f"[Paged Get {self.type}] Endpoint  : {self.url}")
#
#     # Build a list of pagination params to iterate during ingestion.
#     if step_change_version:
#         self.client.verbose_log(
#             f"[Paged Get {self.type}] Pagination Method: Change Version Stepping{' with Reverse-Offset Pagination' if reverse_paging else ''}"
#         )
#
#         paged_params_list = []
#
#         for cv_window_params in self.params.build_change_version_window_params(change_version_step_size):
#             total_count = await self._get_total_count(cv_window_params)
#             cv_offset_params_list = cv_window_params.build_offset_window_params(page_size, total_count=total_count)
#
#             if reverse_paging:
#                 cv_offset_params_list = list(cv_offset_params_list)[::-1]
#
#             paged_params_list.extend(cv_offset_params_list)
#
#     else:
#         self.client.verbose_log(
#             f"[Paged Get {self.type}] Pagination Method: Offset Pagination"
#         )
#
#         total_count = self._get_total_count(self.params)
#         paged_params_list = self.params.build_offset_window_params(page_size, total_count=total_count)
#
#     # Begin pagination-loop
#     for paged_params in paged_params_list:
#
#         ### GET from the API and yield the resulting JSON payload
#         self.client.verbose_log(f"[Paged Get {self.type}] Parameters: {paged_params}")
#
#         if retry_on_failure:
#             res = self._get_response_with_exponential_backoff(
#                 self.url, params=paged_params,
#                 max_retries=max_retries, max_wait=max_wait
#             )
#         else:
#             res = self._get_response(self.url, params=paged_params)
#
#         self.client.verbose_log(f"[Paged Get {self.type}] Retrieved {len(res.json())} rows.")
#         yield res.json()