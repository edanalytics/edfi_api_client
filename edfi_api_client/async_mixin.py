import aiofiles
import aiohttp
import aiohttp_retry
import asyncio
import functools
import itertools
import logging
import os

from collections import defaultdict

from edfi_api_client import util
from edfi_api_client.session import EdFiSession

from typing import Awaitable, AsyncGenerator, Callable, Dict, Iterator, List, Optional, Set, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client import EdFiClient
    from edfi_api_client.params import EdFiParams


class AsyncEdFiSession(EdFiSession):
    """

    """
    retry_status_codes: Set[int] = {401, 429, 500, 501, 503, 504}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session  : Optional[aiohttp.ClientSession] = None
        self.pool_size: Optional[int] = None

        if not (self.client_key and self.client_secret):
            logging.warning("Client key and secret not provided. Async connection with ODS will not be attempted.")
            exit(1)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return await self.session.close()

    async def connect(self,
        pool_size: int = 8,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 1200,
        **kwargs
    ) -> 'AsyncEdFiSession':
        self.pool_size = pool_size

        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.pool_size),
            timeout=aiohttp.ClientTimeout(sock_connect=max_wait),
        )

        if retry_on_failure:
            retry_options = aiohttp_retry.ExponentialRetry(
                attempts=max_retries,
                start_timeout=4.0,  # Note: this logic differs from that of EdFiSession.
                max_timeout=max_wait,
                factor=4.0,
                statuses=self.retry_status_codes,
            )

            self.session = aiohttp_retry.RetryClient(
                client_session=self.session,
                retry_options=retry_options
            )

        # Update time attributes and auth headers with latest authentication information.
        self.authenticate()
        return self


    ### GET Methods
    @EdFiSession._refresh_if_expired
    async def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> aiohttp.ClientResponse:
        """
        Complete an asynchronous GET request against an endpoint URL.

        :param url:
        :param params:
        :return:
        """
        async with self.session.get(
            url, headers=self.auth_headers, params=params,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            self.custom_raise_for_status(response)
            text = await response.text()
            return response

    async def get_total_count(self, url: str, params: 'EdFiParams', **kwargs) -> int:
        """
        This internal helper method is used during pagination.

        :param url:
        :param params:
        :return:
        """
        _params = params.copy()
        _params['totalCount'] = "true"
        _params['limit'] = 0

        res = await self.get_response(url, _params, **kwargs)
        return int(res.headers.get('Total-Count'))


    ### POST Methods
    @EdFiSession._refresh_if_expired
    async def post_response(self, url: str, data: Union[str, dict], **kwargs) -> aiohttp.ClientResponse:
        """
        Complete an asynchronous POST request against an endpoint URL.

        Note: Responses are returned regardless of status.

        :param url:
        :param data:
        :param kwargs:
        :return:
        """
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
    @EdFiSession._refresh_if_expired
    async def delete_response(self, url: str, id: int, **kwargs) -> aiohttp.ClientResponse:
        """
        Complete an asynchronous DELETE request against an endpoint URL.

        :param url:
        :param id:
        :param kwargs:
        :return:
        """
        delete_url = util.url_join(url, id)

        async with self.session.delete(
            delete_url, headers=self.auth_headers,
            verify_ssl=self.verify_ssl, raise_for_status=False
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response


class AsyncEndpointMixin:
    """

    """
    type: str
    client: 'EdFiClient'
    url: str
    params: 'EdFiParams'

    def _run_async_session(func: Callable) -> Callable:
        """
        This decorator establishes an async session before calling the associated class method, if not defined.
        If a session is established at this time, complete a full asyncio run.

        :param func:
        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, session: Optional['AsyncEdFiSession'] = None, **kwargs):
            async def main():
                async with await self.client.async_session.connect(**kwargs) as main_session:
                    return await func(self, *args, session=main_session, **kwargs)

            if session is None:
                return asyncio.run(main())
            else:
                return func(self, *args, session=session, **kwargs)

        return wrapped


    ### GET Methods
    async def async_get_pages(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncGenerator[List[dict], None]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned in pages as a coroutine.

        :param session:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        async def verbose_get_page(param: 'EdFiParams'):
            logging.info(f"[Async Paged Get {self.type}] Parameters: {param}")
            res = await session.get_response(self.url, params=param)
            return await res.json()

        logging.info(f"[Async Paged Get {self.type}] Endpoint  : {self.url}")

        if step_change_version and reverse_paging:
            logging.info(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            logging.info(f"[Async Paged Get {self.type}] Pagination Method: Change Version Stepping")
        else:
            logging.info(f"[Async Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = await self.async_get_paged_window_params(
            session=session,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        for paged_param in paged_params_list:
            yield verbose_get_page(paged_param)

    @_run_async_session
    async def async_get_rows(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> List[dict]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a list in-memory.

        :param session:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        paged_results = self.async_get_pages(
            session=session,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        collected_pages = await self._gather_with_concurrency(
            session.pool_size,
            *[page async for page in paged_results]
        )
        return list(itertools.chain.from_iterable(collected_pages))

    @_run_async_session
    async def async_get_to_json(self,
        path: str,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> str:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are written to a file as JSON lines.

        :param path:
        :param session:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        async def write_async_page(page: Awaitable[List[dict]], fp: 'aiofiles.threadpool'):
            await fp.write(util.page_to_bytes(await page))

        logging.info(f"Writing rows to disk: `{path}`")

        paged_results = self.async_get_pages(
            session=session,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        async with aiofiles.open(path, 'wb') as fp:
            await self._gather_with_concurrency(
                session.pool_size,
                *[write_async_page(page, fp=fp) async for page in paged_results]
            )

        return path

    @_run_async_session
    async def async_get_paged_window_params(self,
        *,
        session: 'AsyncEdFiSession',
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> List['EdFiParams']:
        """

        :param session:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        async def build_total_count_windows(params):
            total_count = await session.get_total_count(self.url, params, **kwargs)
            return params.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging)

        if step_change_version:
            top_level_params = self.params.build_change_version_window_params(change_version_step_size)
        else:
            top_level_params = [self.params]

        nested_params = await self._gather_with_concurrency(session.pool_size, *map(build_total_count_windows, top_level_params))
        return list(itertools.chain.from_iterable(nested_params))


    ### POST Methods
    async def async_post_rows(self,
        rows: Iterator[dict],
        *,
        session: 'AsyncEdFiSession',
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Dict[str, List[int]]:
        """
        This method tries to asynchronously post all rows from an iterator.

        :param rows:
        :param session:
        :param include:
        :param exclude:
        :return:
        """
        output_log = defaultdict(list)

        async def post_and_log(idx: int, row: dict):
            if include and idx not in include:
                return
            elif exclude and idx in exclude:
                return

            try:
                response = await session.post_response(self.url, data=row, **kwargs)
                await self._async_log_response(output_log, idx, response=response)
            except Exception as error:
                await self._async_log_response(output_log, idx, message=error)

        logging.info(f"[Async Post {self.type}] Endpoint  : {self.url}")

        await self._gather_with_concurrency(
            session.pool_size,
            *(post_and_log(idx, row) for idx, row in enumerate(rows))
         )

        # Sort row numbers for easier debugging
        return {key: sorted(val) for key, val in output_log.items()}

    @_run_async_session
    async def async_post_from_json(self,
        path: str,
        *,
        session: 'AsyncEdFiSession',
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Dict[str, List[int]]:
        """

        :param path:
        :param session:
        :param include:
        :param exclude:
        :return:
        """
        def stream_rows(path_: str):
            with open(path_, 'rb') as fp:
                yield from fp

        logging.info(f"Posting rows from disk: `{path}`")

        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")

        return await self.async_post_rows(
            rows=stream_rows(path),
            include=include, exclude=exclude,
            session=session,
            **kwargs
        )


    ### DELETE Methods
    async def async_delete_ids(self, ids: Iterator[int], *, session: 'AsyncEdFiSession', **kwargs):
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :param session:
        :return:
        """
        output_log = defaultdict(list)

        async def delete_and_log(id: int, row: dict):
            try:
                response = await session.delete_response(self.url, id=id, **kwargs)
                await self._async_log_response(output_log, id, response=response)
            except Exception as error:
                await self._async_log_response(output_log, id, message=error)

        logging.info(f"[Async Delete {self.type}] Endpoint  : {self.url}")

        await self._gather_with_concurrency(
            session.pool_size,
            *(delete_and_log(id, row) for id, row in enumerate(ids))
        )

        # Sort row numbers for easier debugging
        return {key: sorted(val) for key, val in output_log.items()}

    @staticmethod
    async def _async_log_response(
        output_log: dict,
        idx: int,
        response: Optional[aiohttp.ClientResponse] = None,
        message: Optional[Exception] = None
    ):
        """
        Helper for updating response output logs consistently.
        """
        if response is not None:
            if response.ok:
                message = f"{response.status_code}"
            else:
                res_json = await response.json()
                message = f"{response.status_code} {res_json.get('message')}"

        message = message or str(message)
        output_log[message].append(idx)


    ### Async Utilities
    @staticmethod
    async def _gather_with_concurrency(n, *tasks, return_exceptions: bool = False) -> list:
        """
        Waits for an entire task queue to finish processing

        :param n:
        :param tasks:
        :param return_exceptions:
        :return:
        """
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                if not isinstance(task, asyncio.Task):
                    task = asyncio.create_task(task)
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks), return_exceptions=return_exceptions)
