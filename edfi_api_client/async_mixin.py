import aiofiles
import aiohttp
import asyncio
import functools
import itertools
import logging

from requests.exceptions import RequestsWarning

from edfi_api_client import util
from edfi_api_client.session import EdFiSession

from typing import Awaitable, AsyncGenerator, Callable, List, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client import EdFiClient
    from edfi_api_client.params import EdFiParams


class AsyncEdFiSession(EdFiSession):
    """

    """
    def __init__(self, *args, pool_size: int = 8, **kwargs):
        super().__init__(*args, pool_size=pool_size, **kwargs)
        self.pool_size = pool_size

        if not (self.client_key and self.client_secret):
            logging.warning("Client key and secret not provided. Async connection with ODS will not be attempted.")
            exit(1)

        self.auth_headers = {}
        self.session: aiohttp.ClientSession = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return await self.session.close()

    async def connect(self, **kwargs) -> 'AsyncEdFiSession':
        if self.session is None:
            self.session = aiohttp.ClientSession(raise_for_status=True, **kwargs)

        # Updates time attributes to match response
        auth_info = self.get_auth_response().json()
        access_token = auth_info['access_token']

        # Update time attributes and auth headers with latest authentication information.
        self.authenticate()

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
        TODO: Process not cancelled upon error.
        TODO: Reauthentication unsuccessful.

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

        async with self.session.get(
            url, headers=self.auth_headers, params=params,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            self.custom_raise_for_status(response)
            text = await response.text()
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
        def wrapped(self,
            *args,
            session: Optional['AsyncEdFiSession'] = None,
            **kwargs
        ):
            if session:
                return func(self, *args, session=session, **kwargs)

            # Otherwise, build the connection and complete a full asyncio run.
            async def main():
                async with await self.client.async_session.connect() as session:
                    return await func(self, *args, session=session, **kwargs)

            return asyncio.run(main())

        return wrapped

    ### GET-all methods
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
    ) -> AsyncGenerator[List[dict], None]:
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
        async def verbose_get_page(param: 'EdFiParams'):
            self.client.verbose_log(f"[Async Paged Get {self.type}] Parameters: {param}")

            res = await session.get_response(
                self.url, params=param,
                retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait
            )
            return await res.json()


        self.client.verbose_log(f"[Async Paged Get {self.type}] Endpoint  : {self.url}")

        if step_change_version and reverse_paging:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping")
        else:
            self.client.verbose_log(f"[Async Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = await self.async_get_paged_window_params(
            session=session,
            page_size=page_size,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            reverse_paging=reverse_paging
        )

        for paged_param in paged_params_list:
            yield verbose_get_page(paged_param)

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
        async def write_async_page(page: Awaitable[List[dict]], fp: 'aiofiles.threadpool'):
            await fp.write(util.page_to_bytes(await page))

        self.client.verbose_log(f"Writing rows to disk: `{path}`")

        paged_results = self.async_get_pages(
            session=session,
            page_size=page_size,
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size, reverse_paging=reverse_paging
        )

        # TODO: The file isn't being created upon 'open'.
        async with aiofiles.open(path, 'wb') as fp:
            await self.gather_with_concurrency(
                session.pool_size,
                *[write_async_page(page, fp=fp) async for page in paged_results]
            )

        return path

    async def async_get_paged_window_params(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int,
        step_change_version: bool,
        change_version_step_size: int,
        reverse_paging: bool
    ) -> Awaitable[List['EdFiParams']]:
        """

        :param session:
        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        async def build_total_count_windows(params):
            total_count = await session.get_total_count(self.url, params)
            return params.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging)


        if step_change_version:
            top_level_params = self.params.build_change_version_window_params(change_version_step_size)
        else:
            top_level_params = [self.params]

        nested_params = await self.gather_with_concurrency(session.pool_size, *map(build_total_count_windows, top_level_params))
        return list(itertools.chain.from_iterable(nested_params))


    ### Async Utilities
    @staticmethod
    async def gather_with_concurrency(n, *tasks) -> list:
        """
        Waits for an entire task queue to finish processing

        :param n:
        :param tasks:
        :return:
        """
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                if not isinstance(task, asyncio.Task):
                    task = asyncio.create_task(task)
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks), return_exceptions=True)