import functools
import logging
import time

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestsWarning

from edfi_api_client import util

from typing import Awaitable, Callable, Optional, Set, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.params import EdFiParams


# Attempt to import optional dependencies.
try:
    import aiofiles
    import aiohttp
    import aiohttp_retry
except ImportError:
    _has_async = False
else:
    _has_async = True


class EdFiSession:
    """

    """
    retry_status_codes: Set[int] = {401, 429, 500, 501, 503, 504}

    def __init__(self,
        oauth_url: str,
        client_key: Optional[str],
        client_secret: Optional[str],
        verify_ssl: bool = True,
        **kwargs
    ):
        self.oauth_url: str = oauth_url
        self.client_key: Optional[str] = client_key
        self.client_secret: Optional[str] = client_secret
        self.verify_ssl: bool = verify_ssl

        # Attributes refresh on connect
        self.authenticated_at: int = None
        self.refresh_at: int = None
        self.auth_headers: dict = {}
        self.session: requests.Session = None

        # Optional retry attributes
        self.retry_on_failure: bool = False
        self.max_retries: int = 5
        self.max_wait: int = 1200

    def __bool__(self) -> bool:
        return bool(self.session)

    def __enter__(self):
        return self

    def __exit__(self):
        self.session.close()
        self.session = None  # Force session to reset between context loops.

    def connect(self,
        retry_on_failure: bool = False,
        max_retries: Optional[int] = None,
        max_wait: Optional[int] = None,
        **kwargs
    ) -> requests.Session:
        """
        Create a session with authorization headers.

        :return:
        """
        # Overwrite retry-configs if passed.
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries or self.max_retries
        self.max_wait = max_wait or self.max_wait

        # Update time attributes and auth headers with latest authentication information.
        self.authenticate()

        self.session = requests.Session()
        self.session.verify = self.verify_ssl  # Only synchronous session uses `verify` attribute.

        return self


    ### Methods to assist in authentication and retries.
    def authenticate(self) -> dict:
        """
        Note: This function is identical in both synchronous and asynchronous sessions.
        """
        # Ensure the connection has been established before trying to refresh.
        if not (self.client_key and self.client_secret):
            raise requests.exceptions.ConnectionError(
                "An established connection to the ODS is required! Provide the client_key and client_secret in EdFiClient arguments."
            )

        # Only re-authenticate when necessary.
        if self.authenticated_at:
            if self.refresh_at < int(time.time()):
                logging.info("Session authentication is expired. Attempting reconnection...")
            else:
                return self.auth_headers

        auth_response = requests.post(
            self.oauth_url,
            auth=HTTPBasicAuth(self.client_key, self.client_secret),
            data={'grant_type': 'client_credentials'},
            verify=self.verify_ssl
        )
        auth_response.raise_for_status()

        # Track when connection was established and when to refresh the access token.
        auth_payload = auth_response.json()
        self.authenticated_at = int(time.time())
        self.refresh_at = int(self.authenticated_at + auth_payload.get('expires_in') - 120)

        self.auth_headers.update({
            'Authorization': f"Bearer {auth_payload.get('access_token')}",
        })
        return self.auth_headers

    def _with_exponential_backoff(func: Callable):
        """
        Decorator to apply exponential backoff during failed requests.
        TODO: Is this logic and status codes consistent across request types?
        TODO: Can this same decorator be used in async, since we cannot have async requests made to overloaded ODS?
        :return:
        """
        @functools.wraps(func)
        def wrapped(self,
            *args,
            retry_on_failure: bool = False,
            max_retries: Optional[int] = None,
            max_wait: Optional[int] = None,
            **kwargs
        ):
            """
            Retry kwargs can be passed during Session connect or on-the-fly during requests.
            """
            if not retry_on_failure or self.retry_on_failure:
                return func(self, *args, **kwargs)

            # Attempt the GET until success or `max_retries` reached.
            max_retries = max_retries or self.max_retries
            max_wait = max_wait or self.max_wait

            for n_tries in range(max_retries):

                try:
                    return func(self, *args, **kwargs)

                except RequestsWarning:
                    # If an API call fails, it may be due to rate-limiting.
                    time.sleep(
                        min((2 ** n_tries) * 2, max_wait)
                    )
                    logging.warning(f"Retry number: {n_tries}")

            # This block is reached only if max_retries has been reached.
            else:
                raise requests.exceptions.RetryError("API retry failed: max retries exceeded for URL.")

        return wrapped


    ### GET Methods
    @_with_exponential_backoff
    def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        response = self.session.get(url, headers=self.auth_headers, params=params)
        self._custom_raise_for_status(response)
        return response


    ### POST Methods
    @_with_exponential_backoff
    def post_response(self, url: str, data: Union[str, dict], **kwargs) -> requests.Response:
        """
        Complete a POST request against an endpoint URL.
        Note: Responses are returned regardless of status.

        :param url:
        :param data:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        post_headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            **self.auth_headers
        }
        data = util.clean_post_row(data)
        return self.session.post(url, headers=post_headers, data=data, **kwargs)


    ### DELETE Methods
    @_with_exponential_backoff
    def delete_response(self, url: str, id: int, **kwargs) -> requests.Response:
        """
        Complete a DELETE request against an endpoint URL.
        Note: Responses are returned regardless of status.

        :param url:
        :param id:
        :param kwargs:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        delete_url = util.url_join(url, id)
        response = self.session.get(delete_url, headers=self.auth_headers, **kwargs)
        return response


    ### Error response methods
    @staticmethod
    def _custom_raise_for_status(response):
        """
        Custom HTTP exception logic and logging.
        The built-in Response.raise_for_status() fails too broadly, even in cases where a connection-reset is enough.

        :param response:
        :return:
        """
        error_messages = {
            400: "400: Bad request. Check your params. Is 'limit' set too high?",
            401: "401: Unauthenticated for URL. The connection may need to be reset.",
            403: "403: Resource not authorized.",
            404: "404: Resource not found.",
            429: "429: Too many requests. The ODS is overwhelmed.",
            500: "500: Internal server error.",
            504: "504: Gateway time-out for URL. The connection may need to be reset.",
        }

        if 400 <= response.status_code < 600:
            logging.warning(f"API Error: {response.status_code} {response.reason}")
            message = error_messages.get(response.status_code, response.reason)  # Default to built-in response message

            if rseponse.status_code in self.retry_status_codes:
                raise RequestsWarning(message)  # Exponential backoff expects a RequestsWarning
            else:
                raise HTTPError(message, response=response)


class AsyncEdFiSession(EdFiSession):
    """

    """
    def __init__(self, *args, pool_size: int = 8, **kwargs):
        """
        EdFiSession initialization sets auth attributes, but does not start a session.
        Session enters event loop on `async_session.connect(**retry_kwargs)`.
        """
        super().__init__(*args, **kwargs)
        self.session  : Optional['ClientSession'] = None
        self.pool_size: int = pool_size

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.session.close()
        self.session = None  # Force session to reset between context loops.

    def connect(self,
        pool_size: Optional[int] = None,
        retry_on_failure: bool = False,
        max_retries: Optional[int] = None,
        max_wait: Optional[int] = None,
        **kwargs
    ) -> 'AsyncEdFiSession':
        # Overwrite retry-configs if passed.
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries or self.max_retries
        self.max_wait = max_wait or self.max_wait
        self.pool_size = pool_size or self.pool_size

        # Update time attributes and auth headers with latest authentication information.
        # Run before any methods that reference optional aiohttp and aiofiles packages.
        self.authenticate()  # Blocking method to make sure authentication happens only once

        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.pool_size),
            timeout=aiohttp.ClientTimeout(sock_connect=self.max_wait),
        )

        if self.retry_on_failure:
            retry_options = aiohttp_retry.ExponentialRetry(
                attempts=self.max_retries,
                start_timeout=4.0,  # Note: this logic differs from that of EdFiSession.
                max_timeout=self.max_wait,
                factor=4.0,
                statuses=self.retry_status_codes,
            )

            self.session = aiohttp_retry.RetryClient(client_session=self.session, retry_options=retry_options)

        return self

    def authenticate(self) -> dict:
        """
        Verify optional async dependencies are installed before authenticating.
        """
        if not _has_async:
            raise ModuleNotFoundError(
                "Asynchronous functionality requires additional packages to be installed. Use `pip install edfi_api_client[async]` to install them."
            )
        return super().authenticate()


    ### GET Methods
    async def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> Awaitable['ClientSession']:
        """
        Complete an asynchronous GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        async with self.session.get(
            url, headers=self.auth_headers, params=params,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            self._custom_raise_for_status(response)
            text = await response.text()
            return response


    ### POST Methods
    async def post_response(self, url: str, data: Union[str, dict], **kwargs) -> Awaitable['ClientResponse']:
        """
        Complete an asynchronous POST request against an endpoint URL.

        Note: Responses are returned regardless of status.

        :param url:
        :param data:
        :param kwargs:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        post_headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            **self.auth_headers
        }
        data = util.clean_post_row(data)

        async with self.session.post(
            url, headers=post_headers, data=data,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response


    ### DELETE Methods
    async def delete_response(self, url: str, id: int, **kwargs) -> Awaitable['ClientResponse']:
        """
        Complete an asynchronous DELETE request against an endpoint URL.

        :param url:
        :param id:
        :param kwargs:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        delete_url = util.url_join(url, id)

        async with self.session.delete(
            delete_url, headers=self.auth_headers,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response
