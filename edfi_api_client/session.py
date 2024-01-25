import functools
import logging
import time

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestsWarning

from edfi_api_client import util

from typing import Callable, Optional, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.params import EdFiParams


class EdFiSession:
    """

    """
    def __init__(self,
        base_url: str,
        client_key: str,
        client_secret: str,
        verify_ssl: bool = True,
        **kwargs
    ):
        self.base_url: str = base_url
        self.client_key: str = client_key
        self.client_secret: str = client_secret
        self.verify_ssl: bool = verify_ssl

        # Attributes refresh on connect
        self.authenticated_at: int = None
        self.refresh_at: int = None
        self.auth_headers: dict = {}
        self.session: requests.Session = None


    ### Methods for connecting to the ODS
    def connect(self) -> requests.Session:
        """
        Create a session with authorization headers.

        :return:
        """
        self.session = requests.Session()
        self.session.verify = self.verify_ssl  # Only synchronous session uses `verify` attribute.

        # Update time attributes and auth headers with latest authentication information.
        self.authenticate()
        return self.session

    def authenticate(self) -> requests.Response:
        """

        :return:
        """
        token_path = 'oauth/token'

        auth_response = requests.post(
            util.url_join(self.base_url, token_path),
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

        return auth_response

    def _refresh_if_expired(func: Callable):
        """
        Reauthenticate automatically before making a request if expired.

        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            if self.refresh_at < int(time.time()):
                logging.info("Session authentication is expired. Attempting reconnection...")
                self.authenticate()
            return func(self, *args, **kwargs)
        return wrapped

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
            max_retries: int = 5,
            max_wait: int = 500,
            **kwargs
        ):
            if not retry_on_failure:
                return func(self, *args, **kwargs)

            # Attempt the GET until success or `max_retries` reached.
            for n_tries in range(max_retries):

                try:
                    return func(self, *args, **kwargs)

                except RequestsWarning:
                    # If an API call fails, it may be due to rate-limiting.
                    # Use exponential backoff to wait, then refresh and try again.
                    time.sleep(
                        min((2 ** n_tries) * 2, max_wait)
                    )
                    logging.warning(f"Retry number: {n_tries}")

            # This block is reached only if max_retries has been reached.
            else:
                raise RuntimeError("API retry failed: max retries exceeded for URL.")

        return wrapped


    ### GET Methods
    @_refresh_if_expired
    @_with_exponential_backoff
    def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        response = self.session.get(url, headers=self.auth_headers, params=params, verify=self.verify_ssl)
        self.custom_raise_for_status(response)
        return response


    ### POST Methods
    @_refresh_if_expired
    @_with_exponential_backoff
    def post_response(self, url: str, data: Union[str, dict], **kwargs) -> requests.Response:
        """
        Complete a POST request against an endpoint URL.
        Note: Responses are returned regardless of status.

        :param url:
        :param data:
        :return:
        """
        post_headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            **self.auth_headers
        }
        data = util.clean_post_row(data)
        return self.session.post(url, headers=post_headers, data=data, verify=self.verify_ssl, **kwargs)


    ### DELETE Methods
    @_refresh_if_expired
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
        delete_url = util.url_join(url, id)
        response = self.session.get(delete_url, headers=self.auth_headers, verify=self.verify_ssl, **kwargs)
        return response


    ### Error response methods
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
            elif response.status_code == 429:
                raise RequestsWarning(
                    "429: Too many requests. The ODS is overwhelmed."
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
