import functools
import logging
import time

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestsWarning

from edfi_api_client import util

from typing import Callable, Optional, Set, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.params import EdFiParams


class EdFiSession:
    """

    """
    retry_status_codes: Set[int] = {401, 429, 500, 501, 503, 504}

    def __init__(self,
        oauth_url: str,
        client_key: Optional[str],
        client_secret: Optional[str],
        **kwargs
    ):
        self.oauth_url: str = oauth_url
        self.client_key: Optional[str] = client_key
        self.client_secret: Optional[str] = client_secret

        # Session attributes refresh on EdFiSession.connect().
        self.session: requests.Session = None
        self.verify_ssl: bool = None
        self.retry_on_failure: bool = None
        self.max_retries: int = None
        self.max_wait: int = None
        self.use_snapshot: bool = False

        # Authentication attributes refresh on EdFiSession.connect().
        self.authenticated_at: int = None
        self.refresh_at: int = None
        self.auth_headers: dict = {}

    def __bool__(self) -> bool:
        return bool(self.session)

    def __enter__(self):
        return self

    def __exit__(self):
        self.session.close()
        self.session = None  # Force session to reset between context loops.

    def connect(self, *,
        retry_on_failure: bool,
        max_retries: int,
        max_wait: int,
        use_snapshot: bool,
        verify_ssl: bool,
        **kwargs
    ) -> requests.Session:
        """
        Create a session with authorization headers.

        :return:
        """
        # Overwrite session attributes.
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self.max_wait = max_wait
        self.use_snapshot = use_snapshot
        self.verify_ssl = verify_ssl

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

        # Apply snapshot header if specified.
        self.auth_headers.update({'Use-Snapshot': str(self.use_snapshot)})

        # Complete authentication.
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
            if not (retry_on_failure or self.retry_on_failure):
                response = func(self, *args, **kwargs)
                self._custom_raise_for_status(response)
                return response

            # Attempt the GET until success or `max_retries` reached.
            max_retries = max_retries or self.max_retries
            max_wait = max_wait or self.max_wait

            response = None  # Save the response between retries to raise after all retries.
            for n_tries in range(max_retries):

                try:
                    response = func(self, *args, **kwargs)
                    self._custom_raise_for_status(response, retry_on_failure=True)
                    return response

                except RequestsWarning as retry_warning:
                    # If an API call fails, it may be due to rate-limiting.
                    sleep_secs = min((2 ** n_tries) * 2, max_wait)
                    logging.warning(f"{retry_warning} Sleeping for {sleep_secs} seconds before retry number {n_tries + 1}...")
                    self.safe_sleep(sleep_secs)

            # This block is reached only if max_retries has been reached.
            else:
                message = "API retry failed: max retries exceeded for URL."
                raise HTTPError(message, response=response)

        return wrapped

    def safe_sleep(self, secs: int):
        """ Sync and async methods require different approaches to sleeping. """
        time.sleep(secs)


    @_with_exponential_backoff
    def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        return self.session.get(url, headers=self.auth_headers, params=params)

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
        return self.session.delete(delete_url, headers=self.auth_headers, **kwargs)

    @_with_exponential_backoff
    def put_response(self, url: str, id: int, data: Union[str, dict], **kwargs) -> requests.Response:
        """
        Complete a PUT request against an endpoint URL
        Note: Responses are returned regardless of status.
        :param url:
        :param id:
        :param data:
        """
        self.authenticate()  # Always try to re-authenticate

        put_url = util.url_join(url, id)
        return self.session.put(put_url, headers=self.auth_headers, json=data, verify=self.verify_ssl, **kwargs)


    ### Error response methods
    def _custom_raise_for_status(self, response, *, retry_on_failure: bool = False):
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
            message = error_messages.get(response.status_code, response.reason)  # Default to built-in response message

            if retry_on_failure and response.status_code in self.retry_status_codes:
                raise RequestsWarning(message)  # Exponential backoff expects a RequestsWarning
            else:
                raise HTTPError(message, response=response)
