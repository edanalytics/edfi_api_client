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

from typing import Awaitable, AsyncIterator, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union
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
    ) -> AsyncIterator[dict]:
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

        async for page in paged_results:
            for row in await page:
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

        os.makedirs(os.path.dirname(path), exist_ok=True)
        async with aiofiles.open(path, 'wb') as fp:
            await self.iterate_taskpool(
                lambda page: write_async_page(page, fp),
                paged_results, pool_size=self.async_session.pool_size
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
    async def async_post(self, data: dict, **kwargs) -> Awaitable[Tuple[Optional[str], Optional[str]]]:
        """
        Initialize a new response log if none provided.
        Start index at zero.
        """
        try:
            response = await self.async_session.post_response(self.url, data=data, **kwargs)
            res_text = await response.text()
            res_json = json.loads(res_text) if res_text else {}
            status, message = response.status, res_json.get('message')
        except Exception as error:
            status, message = None, error

        return status, message

    async def _async_post_and_log(self, key: int, row: dict, *, output_log: ResponseLog, **kwargs) -> ResponseLog:
        """
        Helper to keep async code DRY
        """
        status, message = await self.async_post(row, **kwargs)
        output_log.record(key=key, status=status, message=message)
        output_log.log_progress(self.LOG_EVERY)

    @async_main
    async def async_post_rows(self, rows: AsyncIterator[dict], **kwargs) -> Awaitable[ResponseLog]:
        """
        This method tries to asynchronously post all rows from an iterator.

        :param rows:
        :return:
        """
        logging.info(f"[Async Post {self.component}] Endpoint  : {self.url}")
        output_log = ResponseLog()

        async def aenumerate(iterable: AsyncIterator, start: int = 0):
            n = start
            async for elem in iterable:
                yield n, elem
                n += 1

        await self.iterate_taskpool(
            lambda idx_row: self._async_post_and_log(*idx_row, output_log=output_log, **kwargs),
            aenumerate(rows), pool_size=self.async_session.pool_size
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
        logging.info(f"[Async Post from JSON {self.component}] Posting rows from disk: `{path}`")
        output_log = ResponseLog()

        async def stream_filter_rows(path_: str):
            with open(path_, 'rb') as fp:
                for idx, row in enumerate(fp):

                    if include and idx not in include:
                        continue
                    if exclude and idx in exclude:
                        continue

                    yield idx, row

        if not os.path.exists(path):
            logging.critical("JSON file not found: {path}")
            exit(1)

        await self.iterate_taskpool(
            lambda idx_row: self._async_post_and_log(*idx_row, output_log=output_log, **kwargs),
            stream_filter_rows(path), pool_size=self.async_session.pool_size
        )

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### DELETE Methods
    async def async_delete(self, id: int, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = self.async_session.delete_response(self.url, id=id, **kwargs)
            res_text = await response.text()
            res_json = json.loads(res_text) if res_text else {}
            status, message = response.status, res_json.get('message')
        except Exception as error:
            status, message = None, error

        return status, message

    async def _async_delete_and_log(self, id: int, *, output_log: ResponseLog, **kwargs) -> ResponseLog:
        """
        Helper to keep async code DRY
        """
        status, message = await self.async_delete(id, **kwargs)
        output_log.record(key=id, status=status, message=message)
        output_log.log_progress(self.LOG_EVERY)

    async def async_delete_ids(self, ids: Iterator[int], **kwargs) -> Awaitable[ResponseLog]:
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :return:
        """

        logging.info(f"[Async Delete {self.component}] Endpoint  : {self.url}")
        output_log = ResponseLog()

        await self.iterate_taskpool(
            lambda id: self._async_delete_and_log(id, output_log=output_log, **kwargs),
            ids, pool_size=self.async_session.pool_size
        )

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### Async Utilities
    @staticmethod
    async def iterate_taskpool(callable: Callable[[object], object], iterator: AsyncIterator[object], pool_size: int = 8):
        """

        """
        pending = set()

        async for item in iterator:
            if len(pending) >= pool_size:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            pending.add(asyncio.create_task(callable(item)))

        return await asyncio.wait(pending)
