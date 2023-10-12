import aiofiles
import aiohttp
import asyncio
import functools
import logging

from requests.exceptions import RequestsWarning

from edfi_api_client import util
from edfi_api_client.session import EdFiSession

from typing import AsyncIterator, Callable, List, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client import EdFiClient
    from edfi_api_client.params import EdFiParams


class AsyncEdFiSession(EdFiSession):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not (self.client_key and self.client_secret):
            logging.warning("Client key and secret not provided. Async connection with ODS will not be attempted.")
            exit(1)

        self.session: aiohttp.ClientSession = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return await self.session.close()

    async def connect(self) -> 'AsyncEdFiSession':
        if self.session is None:
            self.session = aiohttp.ClientSession()

        # Updates time attributes to match response
        auth_info = self.get_auth_response().json()
        access_token = auth_info['access_token']

        self.session.headers.update({
            'Authorization': 'Bearer {}'.format(access_token),
        })
        logging.info("Async connection to ODS successful!")
        return self


    ### Elementary GET Methods
    async def get_response(self,
        url: str,
        params: Optional['EdFiParams'] = None,
        *,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 600,
        **kwargs
    ) -> aiohttp.ClientResponse:
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
            return await self.get_response_with_exponential_backoff(url, params, max_retries=max_retries, max_wait=max_wait, **kwargs)

        async with self.session.get(url, params=params, verify_ssl=self.verify_ssl) as response:
            _ = await response.json()
            self.custom_raise_for_status(response)
            return response

    async def get_response_with_exponential_backoff(self,
        url: str,
        params: 'EdFiParams',
        *,
        max_retries: int = 5,
        max_wait: int = 600,
        **kwargs
    ) -> aiohttp.ClientResponse:
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
                return await self.get_response(url, params, **kwargs)

            except RequestsWarning:
                # If an API call fails, it may be due to rate-limiting.
                # Use exponential backoff to wait, then refresh and try again.
                await asyncio.sleep(
                    min((2 ** n_tries) * 2, max_wait)
                )
                logging.warning(f"Retry number: {n_tries}")

        # This block is reached only if max_retries has been reached.
        else:
            logging.warning(f"[Get with Retry Failed] Endpoint  : {url}")
            logging.warning(f"[Get with Retry Failed] Parameters: {params}")
            raise RuntimeError("API GET failed: max retries exceeded for URL.")

    async def get_total_count(self, url: str, params: 'EdFiParams', **kwargs) -> int:
        """
        `total_count()` is accessible by the user and during reverse offset-pagination.
        This internal helper method prevents code needing to be defined twice.

        :param url:
        :param params:
        :return:
        """
        _params = params.copy()
        _params['totalCount'] = "true"
        _params['limit'] = 0

        res = await self.get_response(url, _params, **kwargs)
        return int(res.headers.get('Total-Count'))


class AsyncEndpointMixin:
    """

    """
    type: str
    client: 'EdFiClient'
    url: str
    params: 'EdFiParams'

    def run_async_session(func: Callable) -> Callable:
        """
        This decorator establishes an async session before calling the associated class method, if not defined.
        If a session is established at this time, complete a full asyncio run.

        :param func:
        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, session: Optional['AsyncEdFiSession'] = None, **kwargs):
            if session:
                return func(self, *args, session=session, **kwargs)

            # Otherwise, build the connection and complete a full asyncio run.
            async def main():
                async with await self.client.async_session.connect() as session:
                    return await func(self, *args, session=session, **kwargs)

            return asyncio.run(main())

        return wrapped

    async def async_get_pages(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True,
    ) -> AsyncIterator[List[dict]]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned in pages as a coroutine.

        :param session:
        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        self.client.verbose_log(f"[Async Paged Get {self.type}] Endpoint  : {self.url}")

        if step_change_version and reverse_paging:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping")
        else:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = self.async_get_paged_window_params(
            session=session,
            page_size=page_size,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            reverse_paging=reverse_paging
        )

        # Begin pagination-loop
        async for paged_params in paged_params_list:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Parameters: {paged_params}")

            res = await session.get_response(
                self.url, params=paged_params,
                retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait
            )

            page = await res.json()

            self.client.verbose_log(f"[Async Paged Get {self.type}] Retrieved {len(page)} rows.")
            yield page

    async def async_get_paged_window_params(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int,
        step_change_version: bool,
        change_version_step_size: int,
        reverse_paging: bool
    ) -> AsyncIterator['EdFiParams']:
        """

        :param session:
        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        if step_change_version:
            for cv_window_params in self.params.build_change_version_window_params(change_version_step_size):
                total_count = await session.get_total_count(self.url, cv_window_params)
                cv_offset_params_list = cv_window_params.build_offset_window_params(page_size, total_count=total_count)

                if reverse_paging:
                    cv_offset_params_list = list(cv_offset_params_list)[::-1]

                for param in cv_offset_params_list:
                    yield param
        else:
            total_count = await session.get_total_count(self.url, self.params)
            for param in self.params.build_offset_window_params(page_size, total_count=total_count):
                yield param

    async def async_get_rows(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True
    ) -> AsyncIterator[dict]:
        """
        This method returns all rows from an endpoint, applying pagination logic as necessary.
        Rows are returned as a generator.

        :param session:
        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        paged_result_iter = self.async_get_pages(
            session=session,
            page_size=page_size,
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
        )

        async for paged_result in paged_result_iter:
            for row in paged_result:
                yield row

    @run_async_session
    async def async_get_to_json(self,
        path: str,

        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,

        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 500,

        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        reverse_paging: bool = True,
    ) -> str:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are written to a file as JSON lines.

        :param session:
        :param path:
        :param page_size:
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        self.client.verbose_log(f"Writing rows to disk: `{path}`")

        paged_results = self.async_get_pages(
            session=session,
            page_size=page_size,
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
        )

        async with aiofiles.open(path, 'wb') as fp:
            async for page in paged_results:
                await fp.write(util.page_to_bytes(page))

        return path
