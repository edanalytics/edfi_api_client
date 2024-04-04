import asyncio
import functools
import json
import logging
import os
import requests

from requests import HTTPError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestsWarning

from edfi_api_client import util
from edfi_api_client.response_log import ResponseLog
from edfi_api_client.session import EdFiSession

from typing import Awaitable, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.client import EdFiClient
    from edfi_api_client.params import EdFiParams


# Attempt to import optional dependencies.
try:
    import aiofiles
    import aiohttp
except ImportError:
    _has_async = False
else:
    _has_async = True


class AsyncEdFiSession(EdFiSession):
    """

    """
    def __init__(self, *args, **kwargs):
        """
        EdFiSession initialization sets auth attributes, but does not start a session.
        Session enters event loop on `async_session.connect(**retry_kwargs)`.
        """
        super().__init__(*args, **kwargs)
        self.session: 'aiohttp.ClientSession' = None
        self.pool_size: int = None

        # Build a client-specific non-blocking lock for authentication and retries.
        self.lock: asyncio.Lock = asyncio.Lock()

    async def __aenter__(self) -> 'AsyncEdFiSession':
        return self

    async def __aexit__(self, *exc):
        await self.session.close()
        self.session = None  # Force session to reset between context loops.

    def connect(self, *,
        retry_on_failure: bool,
        max_retries: int,
        max_wait: int,
        verify_ssl: bool,
        pool_size: int,
        **kwargs
    ) -> 'AsyncEdFiSession':
        # Overwrite session attributes.
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self.max_wait = max_wait
        self.verify_ssl = verify_ssl
        self.pool_size = pool_size

        # Update time attributes and auth headers with latest authentication information.
        # Run before any methods that reference optional aiohttp and aiofiles packages.
        self.authenticate()  # Blocking method to make sure authentication happens only once

        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.pool_size),
            timeout=aiohttp.ClientTimeout(sock_connect=self.max_wait),
        )

        return self

    def authenticate(self) -> dict:
        """
        Verify optional async dependencies are installed before authenticating.
        """
        if not _has_async:
            raise ModuleNotFoundError(
                "Asynchronous functionality requires additional packages to be installed."
                "Use `pip install edfi_api_client[async]` to install them."
            )
        return super().authenticate()

    def _async_with_exponential_backoff(func: Callable):
        """
        Decorator to apply exponential backoff during failed requests.
        Future update: unify this with EdFiSession._with_exponential_backoff to keep code DRY.
        """
        @functools.wraps(func)
        async def wrapped(self,
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
                response = await func(self, *args, **kwargs)
                self._custom_raise_for_status(response)
                return response

            # Attempt the GET until success or `max_retries` reached.
            max_retries = max_retries or self.max_retries
            max_wait = max_wait or self.max_wait

            response = None  # Save the response between retries to raise after all retries.
            for n_tries in range(max_retries):

                try:
                    response = await func(self, *args, **kwargs)
                    self._custom_raise_for_status(response, retry_on_failure=True)
                    return response

                except RequestsWarning as retry_warning:
                    # If an API call fails, it may be due to rate-limiting.
                    sleep_secs = min((2 ** n_tries) * 2, max_wait)
                    logging.warning(f"{retry_warning} Sleeping for {sleep_secs} seconds before retry number {n_tries + 1}...")
                    async with self.lock:
                        await asyncio.sleep(sleep_secs)

            # This block is reached only if max_retries has been reached.
            else:
                message = "API retry failed: max retries exceeded for URL."
                raise HTTPError(message, response=response)

        return wrapped


    @_async_with_exponential_backoff
    async def get_response(self,
        url: str,
        params: Optional['EdFiParams'] = None,
        pool_size: Optional[int] = None,  # Ignored optional kwargs argument
        **kwargs
    ) -> Awaitable['aiohttp.ClientSession']:
        """
        Complete an asynchronous GET request against an endpoint URL.

        :param url:
        :param params:
        :param pool_size:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        async with self.session.get(
            url, headers=self.auth_headers, params=params,
            verify_ssl=self.verify_ssl, raise_for_status=False,
            **kwargs
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response

    @_async_with_exponential_backoff
    async def post_response(self,
        url: str,
        data: Union[str, dict],
        pool_size: Optional[int] = None,  # Ignored optional kwargs argument
        **kwargs
    ) -> Awaitable['aiohttp.ClientResponse']:
        """
        Complete an asynchronous POST request against an endpoint URL.

        Note: Responses are returned regardless of status.

        :param url:
        :param data:
        :param pool_size:
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
            verify_ssl=self.verify_ssl, raise_for_status=False,
            **kwargs
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response

    @_async_with_exponential_backoff
    async def delete_response(self,
        url: str,
        id: int,
        pool_size: Optional[int] = None,  # Ignored optional kwargs argument
        **kwargs
    ) -> Awaitable['aiohttp.ClientResponse']:
        """
        Complete an asynchronous DELETE request against an endpoint URL.

        :param url:
        :param id:
        :param pool_size:
        :param kwargs:
        :return:
        """
        self.authenticate()  # Always try to re-authenticate

        delete_url = util.url_join(url, id)

        async with self.session.delete(
            delete_url, headers=self.auth_headers,
            verify_ssl=self.verify_ssl, raise_for_status=False,
            **kwargs
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response

    @_async_with_exponential_backoff
    async def put_response(self,
        url: str,
        id: int,
        data: Union[str, dict],
        pool_size: Optional[int] = None,  # Ignored optional kwargs argument
        **kwargs
    ) -> requests.Response:
        """
        Complete a PUT request against an endpoint URL
        Note: Responses are returned regardless of status.
        :param url:
        :param id:
        :param data:
        :param pool_size:
        :param kwargs:
        """
        self.authenticate()  # Always try to re-authenticate

        put_url = util.url_join(url, id)

        async with self.session.put(
            put_url, headers=self.auth_headers, json=data,
            verify_ssl=self.verify_ssl, raise_for_status=False,
            **kwargs
        ) as response:
            response.status_code = response.status  # requests.Response and aiohttp.ClientResponse use diff attributes
            text = await response.text()
            return response


class AsyncEdFiClientMixin:
    oauth_url: str
    client_key: Optional[str]
    client_secret: Optional[str]
    verify_ssl: bool

    def __init__(self):
        # Async client connects only when called in an async method.
        self.async_session = AsyncEdFiSession(self.oauth_url, self.client_key, self.client_secret)

    def async_connect(self,
        retry_on_failure: bool = False,
        max_retries: int = 5,
        max_wait: int = 1200,
        pool_size: int = 8,
        **kwargs
    ) -> AsyncEdFiSession:
        return self.async_session.connect(
            retry_on_failure=retry_on_failure, max_retries=max_retries, max_wait=max_wait,
            verify_ssl=self.verify_ssl, pool_size=pool_size, **kwargs
        )


class AsyncEdFiEndpointMixin:
    """

    """
    component: str
    url: str
    params: 'EdFiParams'
    client: 'EdFiClient'

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
                async with self.client.async_connect(**kwargs):
                    return await func(self, *args, **kwargs)

            if not self.client.async_session:
                return asyncio.run(main())
            else:
                return func(self, *args, **kwargs)

        return wrapped


    ### GET Methods
    @async_main
    async def async_get_total_count(self, *, params: Optional[dict] = None, **kwargs) -> Awaitable[int]:
        """
        This internal helper method is used during pagination.

        :param params:
        :return:
        """
        logging.info(f"[Async Get Total Count {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = (params or self.params).copy()
        params['totalCount'] = "true"
        params['limit'] = 0

        res = await self.client.async_session.get_response(self.url, params, **kwargs)
        return int(res.headers.get('Total-Count'))

    @async_main
    async def async_get(self,
        limit: Optional[int] = None,
        *,
        params: Optional['EdFiParams'] = None,  # Optional alternative params
        **kwargs
    ) -> Union[Awaitable[List[dict]], List[dict]]:
        """

        """
        logging.info(f"[Async Get {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = (params or self.params).copy()
        if limit:  # Override limit if passed
            params['limit'] = limit

        logging.info(f"[Async Get {self.component}] Parameters: {params}")
        res = await self.client.async_session.get_response(self.url, params=params, **kwargs)
        return await res.json()

    async def async_get_pages(self,
        *,
        params: Optional['EdFiParams'] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncIterator[List[dict]]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned in pages as a coroutine.

        :param params:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        if step_change_version and reverse_paging:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Change Version Stepping")
        else:
            logging.info(f"[Async Paged Get {self.component}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = self._async_get_paged_window_params(
            params=(params or self.params).copy(),
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        async for paged_param in paged_params_list:
            yield self.async_get(params=paged_param)

    async def async_get_rows(self,
        *,
        params: Optional['EdFiParams'] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncIterator[dict]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a list in-memory.

        :param params:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        paged_results = self.async_get_pages(
            params=params,
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
        params: Optional['EdFiParams'] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Union[Awaitable[str], str]:
        """
        This method completes a series of asynchronous GET requests, paginating params as necessary based on endpoint.
        Rows are written to a file as JSON lines.

        :param params:
        :param path:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        logging.info(f"[Async Get to JSON {self.component}] Filepath: `{path}`")

        async def write_async_page(page: Awaitable[List[dict]], fp: 'aiofiles.threadpool'):
            """ There are no asynchronous lambdas in Python. """
            await fp.write(util.page_to_bytes(await page))

        paged_results = self.async_get_pages(
            params=params,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        os.makedirs(os.path.dirname(path), exist_ok=True)
        async with aiofiles.open(path, 'wb') as fp:
            await self.iterate_taskpool(
                lambda page: write_async_page(page, fp),
                paged_results, pool_size=self.client.async_session.pool_size
            )

        return path

    async def _async_get_paged_window_params(self,
        *,
        params: 'EdFiParams',
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> AsyncIterator['EdFiParams']:
        """

        :param params:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        if step_change_version:
            top_level_params = params.build_change_version_window_params(change_version_step_size)
        else:
            top_level_params = [params]

        for top_level_param in top_level_params:
            total_count = await self.async_get_total_count(params=top_level_param, **kwargs)
            for window_params in top_level_param.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging):
                yield window_params


    ### POST Methods
    @async_main
    async def async_post(self, data: dict, **kwargs) -> Awaitable[Tuple[Optional[str], Optional[str]]]:
        """
        Initialize a new response log if none provided.
        Start index at zero.
        """
        try:
            response = await self.client.async_session.post_response(self.url, data=data, **kwargs)
            res_text = await response.text()
            res_json = json.loads(res_text) if res_text else {}
            status, message = response.status, res_json.get('message')
        except HTTPError as error:
            status, message = error.response.status_code, error.response.reason
        except Exception as error:
            status, message = None, error

        return status, message

    @async_main
    async def async_post_rows(self,
        rows: Optional[AsyncIterator[dict]] = None,
        *,
        log_every: int = 500,
        id_rows: Optional[Union[Dict[int, dict], Iterator[Tuple[int, dict]]]] = None,
        **kwargs
    ) -> Awaitable[ResponseLog]:
        """
        This method tries to asynchronously post all rows from an iterator.

        :param rows:
        :param log_every:
        :param id_rows: Alternative input iterator argument
        :return:
        """
        logging.info(f"[Async Post {self.component}] Endpoint: {self.url}")
        output_log = ResponseLog(log_every)

        async def post_and_log(key: int, row: dict):
            status, message = await self.async_post(row, **kwargs)
            output_log.record(key=key, status=status, message=message)

        # Argument checking into id_rows: Iterator[(int, dict)]
        if rows and id_rows:
            raise ValueError("Arguments `rows` and `id_rows` are mutually-exclusive.")
        elif rows:
            id_rows = self.aenumerate(self.aiterate(rows))
        elif isinstance(id_rows, dict):  # If a dict, the object is already in memory.
            id_rows = self.aiterate(id_rows.items())

        await self.iterate_taskpool(
            lambda idx_row: post_and_log(*idx_row),
            id_rows, pool_size=self.client.async_session.pool_size
        )

        output_log.log_progress()  # Always log on final count.
        return output_log

    @async_main
    async def async_post_from_json(self,
        path: str,
        *,
        log_every: int = 500,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Union[Awaitable[ResponseLog], ResponseLog]:
        """

        :param path:
        :param log_every:
        :param include:
        :param exclude:
        :return:
        """
        logging.info(f"[Async Post from JSON {self.component}] Posting rows from disk: `{path}`")

        return await self.async_post_rows(
            id_rows=self.aiterate(util.stream_filter_rows(path, include=include, exclude=exclude)),
            log_every=log_every, **kwargs
        )


    ### DELETE Methods
    @async_main
    async def async_delete(self, id: int, **kwargs) -> Awaitable[Tuple[Optional[str], Optional[str]]]:
        try:
            response = await self.client.async_session.delete_response(self.url, id=id, **kwargs)
            res_text = await response.text()
            res_json = json.loads(res_text) if res_text else {}
            status, message = response.status, res_json.get('message')
        except HTTPError as error:
            status, message = error.response.status_code, error.response.reason
        except Exception as error:
            status, message = None, error

        return status, message

    @async_main
    async def async_delete_ids(self, ids: AsyncIterator[int], *, log_every: int = 500, **kwargs) -> Awaitable[ResponseLog]:
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :param log_every:
        :return:
        """
        logging.info(f"[Async Delete {self.component}] Endpoint: {self.url}")
        output_log = ResponseLog(log_every)

        async def delete_and_log(id: int):
            status, message = await self.async_delete(id, **kwargs)
            output_log.record(key=id, status=status, message=message)

        await self.iterate_taskpool(
            delete_and_log, self.aiterate(ids),
            pool_size=self.client.async_session.pool_size
        )

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### PUT Methods
    @async_main
    async def async_put(self, id: int, data: dict, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = await self.client.async_session.put_response(self.url, id=id, data=data, **kwargs)
            res_text = await response.text()
            res_json = json.loads(res_text) if res_text else {}
            status, message = response.status_code, res_json.get('message')
        except HTTPError as error:
            status, message = error.response.status_code, error.response.reason
        except Exception as error:
            status, message = None, error

        return status, message

    @async_main
    async def async_put_id_rows(self,
        id_rows: Union[Dict[int, dict], Iterator[Tuple[int, dict]]],
        log_every: int = 500,
        **kwargs
    ) -> ResponseLog:
        """
        Delete all records at the endpoint by ID.

        :param id_rows:
        :param log_every:
        :return:
        """
        logging.info(f"[Put {self.component}] Endpoint: {self.url}")
        output_log = ResponseLog(log_every)

        async def put_and_log(id: int, row: dict):
            status, message = await self.async_put(id, row, **kwargs)
            output_log.record(key=id, status=status, message=message)

        if isinstance(id_rows, dict):  # If a dict, the object is already in memory.
            id_rows = list(id_rows.items())

        await self.iterate_taskpool(
            lambda id_row: put_and_log(*id_row),
            id_rows, pool_size=self.client.async_session.pool_size
        )

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### Async Utilities
    @staticmethod
    async def iterate_taskpool(callable: Callable[[object], object], iterator: AsyncIterator[object], pool_size: int = 8):
        """
        Alternative to `asyncio.gather()`. Does not require all awaitables to be defined in memory at once.
        """
        pending = set()

        async for item in iterator:
            if len(pending) >= pool_size:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            pending.add(asyncio.create_task(callable(item)))

        return await asyncio.wait(pending)

    @staticmethod
    async def aiterate(iterable: Iterator):
        """ Iterator wrapper that accepts both sync and async iterators. """
        try:
            async for elem in iterable:
                yield elem
        except Exception:
            for elem in iterable:
                yield elem

    @staticmethod
    async def aenumerate(iterable: AsyncIterator, start: int = 0):
        n = start
        async for elem in iterable:
            yield n, elem
            n += 1
