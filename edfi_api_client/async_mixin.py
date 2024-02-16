import aiofiles
import aiohttp
import aiohttp_retry
import asyncio
import functools
import itertools
import json
import logging
import os

from collections import defaultdict

from edfi_api_client import util
from edfi_api_client.session import EdFiSession
from edfi_api_client.response_log import ResponseLog

from typing import Awaitable, AsyncGenerator, Callable, Dict, Iterator, List, Optional, Set, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.params import EdFiParams


class AsyncEdFiSession(EdFiSession):
    """

    """
    retry_status_codes: Set[int] = {401, 429, 500, 501, 503, 504}

    def __init__(self, *args, **kwargs):
        """
        EdFiSession initialization sets auth attributes, but does not start a session.
        Session enters event loop on `async_session.connect(**retry_kwargs)`.
        """
        super().__init__(*args, **kwargs)
        self.session  : Optional[aiohttp.ClientSession] = None
        self.pool_size: Optional[int] = None

        if not (self.client_key and self.client_secret):
            logging.critical("Client key and secret not provided. Async connection with ODS will not be attempted.")
            exit(1)

    def __bool__(self):
        return bool(self.session)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.session.close()
        self.session = None  # Force session to reset between context loops.

    def connect(self,
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
        self.authenticate()  # Blocking method to make sure authentication happens only once
        return self


    ### GET Methods
    @EdFiSession._refresh_if_expired
    async def get_response(self, url: str, params: Optional['EdFiParams'] = None, **kwargs) -> Awaitable[aiohttp.ClientResponse]:
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
            self._custom_raise_for_status(response)
            text = await response.text()
            return response


    ### POST Methods
    @EdFiSession._refresh_if_expired
    async def post_response(self, url: str, data: Union[str, dict], **kwargs) -> Awaitable[aiohttp.ClientResponse]:
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
    async def delete_response(self, url: str, id: int, **kwargs) -> Awaitable[aiohttp.ClientResponse]:
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
    component: str
    async_session: AsyncEdFiSession
    url: str
    params: 'EdFiParams'

    LOG_EVERY: int

    def async_main(func: Callable) -> Callable:
        """
        This decorator establishes an async session before calling the associated class method, if not defined.
        If a session is established at this time, complete a full asyncio run.

        :param func:
        :return:
        """
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs) -> Union[object, Awaitable[object]]:
            async def main():
                async with self.async_session.connect(**kwargs):
                    return await func(self, *args, **kwargs)

            if not self.async_session:
                return asyncio.run(main())
            else:
                return func(self, *args, **kwargs)

        return wrapped


    ### GET Methods
    async def async_get_pages(self,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncIterator[List[dict]]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned in pages as a coroutine.

        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        async def verbose_get_page(param: 'EdFiParams'):
            logging.info(f"[Async Paged Get {self.component}] Parameters: {param}")
            res = await self.async_session.get_response(self.url, params=param)
            return await res.json()

        logging.info(f"[Async Paged Get {self.component}] Endpoint  : {self.url}")

        if step_change_version and reverse_paging:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Change Version Stepping")
        else:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = self._async_get_paged_window_params(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        async for paged_param in paged_params_list:
            yield verbose_get_page(paged_param)

    async def async_get_rows(self,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncGenerator[List[dict], None]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a list in-memory.

        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        paged_results = self.async_get_pages(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        collected_pages = await self._gather_with_concurrency(
            self.async_session.pool_size,
            *[page async for page in paged_results]
        )
        for row in itertools.chain.from_iterable(collected_pages):
            yield row

    @async_main
    async def async_get_to_json(self,
        path: str,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Union[Awaitable[str], str]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are written to a file as JSON lines.

        :param path:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        async def write_async_page(page: Awaitable[List[dict]], fp: 'aiofiles.threadpool'):
            await fp.write(util.page_to_bytes(await page))

        logging.info(f"[Async Get to JSON {self.component}] Filepath: `{path}`")

        paged_results = self.async_get_pages(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        async with aiofiles.open(path, 'wb') as fp:
            await self._gather_with_concurrency(
                self.async_session.pool_size,
                *[write_async_page(page, fp=fp) async for page in paged_results]
            )

        return path

    @async_main
    async def async_get_total_count(self, *, params: Optional[dict] = None, **kwargs) -> Awaitable[int]:
        """
        This internal helper method is used during pagination.

        :param params:
        :return:
        """
        params = (params or self.params).copy()
        params['totalCount'] = "true"
        params['limit'] = 0

        res = await self.async_session.get_response(self.url, params, **kwargs)
        return int(res.headers.get('Total-Count'))

    async def _async_get_paged_window_params(self,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncIterator['EdFiParams']:
        """

        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        if step_change_version:
            top_level_params = self.params.build_change_version_window_params(change_version_step_size)
        else:
            top_level_params = [self.params]

        for params in top_level_params:
            total_count = await self.async_get_total_count(params=params, **kwargs)
            for window_params in params.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging):
                yield window_params


    ### POST Methods
    @async_main
    async def async_post_rows(self,
        rows: Iterator[dict],
        *,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Awaitable[ResponseLog]:
        """
        This method tries to asynchronously post all rows from an iterator.

        :param rows:
        :param include:
        :param exclude:
        :return:
        """
        output_log = ResponseLog()

        async def post_and_log(idx: int, row: dict):
            if include and idx not in include:
                return
            elif exclude and idx in exclude:
                return

            try:
                response = await self.async_session.post_response(self.url, data=row, **kwargs)
                res_text = await response.text()
                res_json = json.loads(res_text) if res_text else {}
                output_log.record(idx, status=response.status, message=res_json.get('message'))
            except Exception as error:
                output_log.record(idx, message=error)
            finally:
                output_log.log_progress(self.LOG_EVERY)

        logging.info(f"[Async Post {self.component}] Endpoint  : {self.url}")

        await self._gather_with_concurrency(
            self.async_session.pool_size,
            *(post_and_log(idx, row) for idx, row in enumerate(rows))
         )

        output_log.log_progress()  # Always log on final count.
        return output_log

    @async_main
    async def async_post_from_json(self,
        path: str,
        *,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Union[ResponseLog, Awaitable[ResponseLog]]:
        """

        :param path:
        :param include:
        :param exclude:
        :return:
        """
        def stream_rows(path_: str):
            with open(path_, 'rb') as fp:
                yield from fp

        logging.info(f"[Async Post from JSON {self.component}] Posting rows from disk: `{path}`")

        if not os.path.exists(path):
            logging.critical("JSON file not found: {path}")
            exit(1)

        return await self.async_post_rows(
            rows=stream_rows(path),
            include=include, exclude=exclude,
            **kwargs
        )


    ### DELETE Methods
    async def async_delete_ids(self, ids: Iterator[int], **kwargs) -> Awaitable[ResponseLog]:
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :return:
        """
        output_log = ResponseLog()

        async def delete_and_log(id: int, row: dict):
            try:
                response = await self.async_session.delete_response(self.url, id=id, **kwargs)
                res_text = await response.text()
                res_json = json.loads(res_text) if res_text else {}
                output_log.record(idx, status=response.status, message=res_json.get('message'))
            except Exception as error:
                output_log.record(id, message=error)
            finally:
                output_log.log_progress(self.LOG_EVERY)

        logging.info(f"[Async Delete {self.component}] Endpoint  : {self.url}")

        await self._gather_with_concurrency(
            self.async_session.pool_size,
            *(delete_and_log(id, row) for id, row in enumerate(ids))
        )

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### Async Utilities
    @staticmethod
    async def _gather_with_concurrency(n, *tasks, return_exceptions: bool = False) -> Awaitable[List[asyncio.Task]]:
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
