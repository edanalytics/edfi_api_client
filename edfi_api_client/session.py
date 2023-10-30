import logging
import time

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestsWarning

from edfi_api_client import util

from typing import Optional
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

    def refresh_if_expired(self):
        if self.refresh_at < int(time.time()):
            logging.info("Session authentication is expired. Attempting reconnection...")
            self.authenticate()


    ### Elementary GET Methods
    def get_response(self,
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
        self.refresh_if_expired()

        if retry_on_failure:
            return self.get_response_with_exponential_backoff(url, params, max_retries=max_retries, max_wait=max_wait, **kwargs)

        response = self.session.get(url, headers=self.auth_headers, params=params, verify=self.verify_ssl)
        self.custom_raise_for_status(response)
        return response

    def get_response_with_exponential_backoff(self,
        url: str,
        params: 'EdFiParams',
        *,
        max_retries,
        max_wait,
        **kwargs
    ) -> requests.Response:
        """
        Complete a GET request against an endpoint URL.
        In the case of failure, retry with exponential backoff until max_retries or max_wait has been exceeded.

        :param url:
        :param params:
        :param max_retries:
        :param max_wait:
        :param kwargs: GET arguments
        :return:
        """
        # Attempt the GET until success or `max_retries` reached.
        for n_tries in range(max_retries):

            try:
                return self.get_response(url, params, **kwargs)

            except RequestsWarning:
                # If an API call fails, it may be due to rate-limiting.
                # Use exponential backoff to wait, then refresh and try again.
                time.sleep(
                    min((2 ** n_tries) * 2, max_wait)
                )
                logging.warning(f"Retry number: {n_tries}")

        # This block is reached only if max_retries has been reached.
        else:
            logging.warning(f"[Get with Retry Failed] Endpoint  : {url}")
            logging.warning(f"[Get with Retry Failed] Parameters: {params}")
            raise RuntimeError("API GET failed: max retries exceeded for URL.")

    def get_total_count(self, url: str, params: 'EdFiParams', **kwargs):
        """
        `total_count()` is accessible by the user and during reverse offset-pagination.
        This internal helper method prevents code needing to be defined twice.

        :param url:
        :param params:
        :return:
        """
        _params = params.copy()  # Don't mutate params in place
        _params['totalCount'] = True
        _params['limit'] = 0

        res = self.get_response(url, _params, **kwargs)
        return int(res.headers.get('Total-Count'))

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
