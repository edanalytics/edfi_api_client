import logging
import os
import requests

from edfi_api_client import util
from edfi_api_client.async_mixin import AsyncEdFiEndpointMixin
from edfi_api_client.params import EdFiParams
from edfi_api_client.response_log import ResponseLog

from typing import Dict, Iterator, List, Optional, Tuple, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.client import EdFiClient
    from edfi_api_client.swagger import EdFiSwagger


class EdFiEndpoint(AsyncEdFiEndpointMixin):
    """
    This is an abstract class for interacting with Ed-Fi resources and descriptors.
    Composites override with custom composite-logic.
    """
    component: str = None

    def __init__(self,
        endpoint_url: str,
        name: str,

        *,
        namespace: str = 'ed-fi',
        get_deletes: bool = False,
        params: Optional[dict] = None,

        # Import the client directly to ensure we use the latest sessions when making requests.
        client: Optional['EdFiClient'] = None,
        swagger: Optional['EdFiSwagger'] = None,
        **kwargs
    ):
        # Hide the intermediate endpoint-URL to prevent confusion
        self._endpoint_url = endpoint_url

        # Names can be passed manually or as a `(namespace, name)` tuple as output from Swagger.
        self.namespace, self.name = self._parse_names(namespace, name)

        # Build URL and dynamic params object
        self.get_deletes: bool = get_deletes
        self.params = EdFiParams(params, **kwargs)

        # Optional helper classes with lazy attributes
        self.client: 'EdFiClient' = client
        self.swagger: 'EdFiSwagger' = swagger

    def __repr__(self):
        """
        Endpoint (Deletes) (with {N} parameters) [{namespace}/{name}]
        """
        deletes_string = " Deletes" if self.get_deletes else ""
        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        return f"<{self.component}{deletes_string}{params_string} [{self.raw}]>"


    ### Naming and Pathing Methods
    @staticmethod
    def _parse_names(namespace: str, name: str) -> Tuple[str, str]:
        """
        Name and namespace can be passed manually or as a `(namespace, name)` tuple as output from Swagger.
        """
        if isinstance(name, str):
            return namespace, util.snake_to_camel(name)
        elif len(name) == 2:
            return name
        else:
            logging.error("Arguments `namespace` and `name` must be passed explicitly, or as a `(namespace, name)` tuple.")

    @property
    def raw(self) -> str:
        return f"{util.snake_to_camel(self.namespace)}/{util.snake_to_camel(self.name)}"

    @property
    def url(self) -> str:
        """
        Build the name/descriptor URL to GET from the API.

        :return:
        """
        # Deletes are an optional path addition.
        deletes = 'deletes' if self.get_deletes else None

        return util.url_join(
            self._endpoint_url,
            self.namespace, self.name, deletes
        )


    ### Lazy swagger attributes
    @property
    def definition_id(self) -> str:
        ns = util.snake_to_camel(self.namespace)
        ep = util.plural_to_singular(self.name)
        return f"{ns}_{ep}"

    @property
    def definition(self) -> dict:
        """
        Snake-to-camel cannot handle certain namespaces, but definitions are thankfully case-agnostic.
        e.g., Namespace: ed-fi-xassessment-roster: (Expected: edFiXassessmentRoster; Actual: edFiXAssessmentRoster)
        """
        definitions = {id.lower(): define for id, define in self.swagger.definitions.items()}
        return definitions.get(self.definition_id.lower(), {})

    @property
    def fields(self) -> List[str]:
        return list(self.field_dtypes.keys())

    @property
    def field_dtypes(self) -> Dict[str, str]:
        return dict(self.definition.get('field_dtypes', {}))  # Force to dict from defaultdict.

    @property
    def required_fields(self) -> List[str]:
        return self.definition.get('required', [])

    @property
    def identity_fields(self) -> List[str]:
        return self.definition.get('identity', [])

    @property
    def has_deletes(self) -> bool:
        return self.swagger.get_endpoint_deletes().get((self.namespace, self.name))

    @property
    def description(self) -> Optional[str]:
        return self.swagger.get_endpoint_descriptions().get(self.name)


    ### Session API methods
    def ping(self, *, params: Optional[dict] = None, **kwargs) -> requests.Response:
        """
        This method pings the endpoint and verifies it is accessible.

        :return:
        """
        logging.info(f"[Ping {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = (params or self.params).copy()
        params['limit'] = 1  # To ping a composite, a limit of at least one is required.

        # We do not want to surface student-level data during ODS-checks.
        res = self.client.session.get_response(self.url, params=params, **kwargs)
        if res.ok:
            res._content = b'{"message": "Ping was successful! ODS data has been intentionally scrubbed from this response."}'

        return res

    def get_total_count(self, *, params: Optional[dict] = None, **kwargs) -> int:
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        logging.info(f"[Get Total Count {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = (params or self.params).copy()
        params['totalCount'] = True
        params['limit'] = 0

        logging.info(f"[Get Total Count {self.component}] Parameters: {params}")
        res = self.client.session.get_response(self.url, params, **kwargs)
        return int(res.headers.get('Total-Count'))

    def total_count(self, *args, **kwargs) -> int:
        logging.warning("`EdFiEndpoint.total_count()` is deprecated. Use `EdFiEndpoint.get_total_count()` instead.")
        return self.get_total_count(*args, **kwargs)


    ### GET Methods
    def get(self, limit: Optional[int] = None, *, params: Optional[dict] = None, **kwargs) -> List[dict]:
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        logging.info(f"[Get {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = (params or self.params).copy()
        if limit:  # Override limit if passed
            params['limit'] = limit

        logging.info(f"[Get {self.component}] Parameters: {params}")
        return self.client.session.get_response(self.url, params=params, **kwargs).json()

    def get_pages(self,
        *,
        params: Optional[dict] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a generator.

        :param params:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        if step_change_version and reverse_paging:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Change Version Stepping")
        else:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = self._get_paged_window_params(
            params=(params or self.params).copy(),
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        for paged_params in paged_params_list:
            paged_rows = self.get(params=paged_params, **kwargs)
            logging.info(f"[Get {self.component}] Retrieved {len(paged_rows)} rows.")
            yield paged_rows

    def get_rows(self,
        *,
        params: Optional[dict] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Iterator[dict]:
        """
        This method returns all rows from an endpoint, applying pagination logic as necessary.
        Rows are returned as a generator.

        :param params:
        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        paged_result_iter = self.get_pages(
            params=params,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        for paged_result in paged_result_iter:
            yield from paged_result

    def get_to_json(self,
        path: str,
        *,
        params: Optional[dict] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> str:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are written to a file as JSON lines.

        :param path:
        :param params:
        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        logging.info(f"[Get to JSON {self.component}] Filepath: `{path}`")

        paged_results = self.get_pages(
            params=params,
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as fp:
            for page in paged_results:
                fp.write(util.page_to_bytes(page))

        return path

    def _get_paged_window_params(self,
        *,
        params: EdFiParams,
        page_size: int,
        reverse_paging: bool,
        step_change_version: bool,
        change_version_step_size: int,
        **kwargs
    ) -> Iterator[EdFiParams]:
        """
        :param params:
        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        if step_change_version:
            for cv_window_params in params.build_change_version_window_params(change_version_step_size):
                total_count = self.get_total_count(params=cv_window_params, **kwargs)
                cv_offset_params_list = cv_window_params.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging)
                yield from cv_offset_params_list
        else:
            total_count = self.get_total_count(params=params, **kwargs)
            yield from params.build_offset_window_params(page_size, total_count=total_count)


    ### POST Methods
    def post(self, data: dict, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        """
        Initialize a new response log if none provided.
        Start index at zero.
        """
        try:
            response = self.client.session.post_response(self.url, data=data, **kwargs)
            res_json = response.json() if response.text else {}
            status, message = response.status_code, res_json.get('message')
        except Exception as error:
            status, message = None, error

        return status, message

    def post_rows(self,
        rows: Optional[Iterator[dict]] = None,
        *,
        log_every: int = 500,
        id_rows: Optional[Union[Dict[int, dict], Iterator[Tuple[int, dict]]]] = None,
        **kwargs
    ) -> ResponseLog:
        """
        This method tries to post all rows from an iterator.

        :param rows:
        :param log_every:
        :param id_rows: Alternative input iterator argument
        :return:
        """
        logging.info(f"[Post {self.component}] Endpoint: {self.url}")
        output_log = ResponseLog(log_every)

        # Argument checking into id_rows: Iterator[(int, dict)]
        if rows and id_rows:
            raise ValueError("Arguments `rows` and `id_rows` are mutually-exclusive.")
        elif rows:
            id_rows = enumerate(rows)
        elif isinstance(id_rows, dict):  # If a dict, the object is already in memory.
            id_rows = list(id_rows.items())

        for id, row in id_rows:
            status, message = self.post(row, **kwargs)
            output_log.record(key=id, status=status, message=message)

        output_log.log_progress()  # Always log on final count.
        return output_log

    def post_from_json(self,
        path: str,
        *,
        log_every: int = 500,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> ResponseLog:
        """

        :param path:
        :param log_every:
        :param include:
        :param exclude:
        :return:
        """
        logging.info(f"[Post from JSON {self.component}] Filepath: `{path}`")

        return self.post_rows(
            id_rows=util.stream_filter_rows(path, include=include, exclude=exclude),
            log_every=log_every, **kwargs
        )


    ### DELETE Methods
    def delete(self, id: int, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = self.client.session.delete_response(self.url, id=id, **kwargs)
            res_json = response.json() if response.text else {}
            status, message = response.status_code, res_json.get('message')
        except Exception as error:
            status, message = None, error

        return status, message

    def delete_ids(self, ids: Iterator[int], *, log_every: int = 500, **kwargs) -> ResponseLog:
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :param log_every:
        :return:
        """
        logging.info(f"[Delete {self.component}] Endpoint: {self.url}")
        output_log = ResponseLog(log_every)

        for id in ids:
            status, message = self.delete(id, **kwargs)
            output_log.record(key=id, status=status, message=message)

        output_log.log_progress()  # Always log on final count.
        return output_log


    ### PUT Methods
    def put(self, id: int, data: dict, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        try:
            response = self.client.session.put_response(self.url, id=id, data=data, **kwargs)
            res_json = response.json() if response.text else {}
            status, message = response.status_code, res_json.get('message')
        except Exception as error:
            status, message = None, error

        return status, message

    def put_id_rows(self,
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

        if isinstance(id_rows, dict):  # If a dict, the object is already in memory.
            id_rows = list(id_rows.items())

        for id, data in id_rows:
            status, message = self.put(id=id, data=data, **kwargs)
            output_log.record(key=id, status=status, message=message)

        output_log.log_progress()  # Always log on final count.
        return output_log


class EdFiResource(EdFiEndpoint):
    component: str = 'Resource'


class EdFiDescriptor(EdFiEndpoint):
    component: str = 'Descriptor'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.get_deletes:
            logging.warning("Descriptors do not have /deletes endpoints. Argument `get_deletes` has been ignored.")


class EdFiComposite(EdFiEndpoint):
    """

    """
    component: str = 'Composite'

    def __init__(self,
        *args,
        composite: str = 'enrollment',
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,
        **kwargs
    ):
        # Assign composite-specific arguments that are used in `self.url`.
        self.composite: str = composite
        self.filter_type: Optional[str] = filter_type
        self.filter_id: Optional[str] = filter_id

        # Init after to build 'self.url' with new attributes
        super().__init__(*args, **kwargs)

        if self.get_deletes:
            logging.warning("Composites do not have /deletes endpoints. Argument `get_deletes` has been ignored.")

    def __repr__(self):
        """
        Enrollment Composite                     [{namespace}/{name}]
                             with {N} parameters                      (filtered on {filter_type})
        """
        composite = self.composite.title()
        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        filter_string = f" (filtered on {self.filter_type})" if self.filter_type else ""
        return f"<{composite} Composite{params_string} [{self.raw}]{filter_string}>"

    @property
    def url(self) -> str:
        """
        Build the composite URL to GET from the API.

        :return:
        """
        # If a filter is applied, the URL changes to match the filter type.
        if self.filter_type is None and self.filter_id is None:
            return util.url_join(
                self._endpoint_url,
                self.namespace, self.composite, self.name.title()
            )
        elif self.filter_type is not None and self.filter_id is not None:
            return util.url_join(
                self._endpoint_url,
                self.namespace, self.composite,
                self.filter_type, self.filter_id, self.name
            )
        else:
            raise ValueError("`filter_type` and `filter_id` must both be specified if a filter is being applied!")

    def get_total_count(self, *args, **kwargs):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        raise NotImplementedError("Total counts have not been implemented in Ed-Fi composites!")

    def get_pages(self, *, params: Optional[dict] = None, page_size: int = 100, **kwargs) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        This is the original logic used before total-count paged-param stepping.
        Rows are returned as a generator.

        :param params:
        :param page_size:
        :return:
        """
        if kwargs.get('step_change_version'):
            logging.warning("Change versions are not implemented in composites! Change version stepping arguments are ignored.")

        logging.info(f"[Paged Get {self.component}] Endpoint: {self.url}")
        logging.info(f"[Paged Get {self.component}] Pagination Method: Offset Pagination")

        # Reset pagination parameters
        paged_params = (params or self.params).copy()
        paged_params.init_page_by_offset(page_size)

        # Begin pagination-loop
        while True:

            ### GET from the API and yield the resulting JSON payload
            logging.info(f"[Paged Get {self.component}] Parameters: {paged_params}")
            res = self.client.session.get_response(self.url, params=paged_params, **kwargs)

            # If rows have been returned, there may be more to ingest.
            if res.json():
                logging.info(f"[Paged Get {self.component}] Retrieved {len(res.json())} rows.")
                yield res.json()

                logging.info(f"    @ Paginating offset...")
                paged_params.page_by_offset(page_size)

            # If no rows are returned, end pagination.
            else:
                logging.info(f"[Paged Get {self.component}] @ Retrieved zero rows. Ending pagination.")
                break

    def post(self, *args, **kwargs):
        raise NotImplementedError("Rows cannot be posted to a composite directly!")

    def delete(self, *args, **kwargs):
        raise NotImplementedError("Rows cannot be deleted from a composite directly!")

    def put(self, *args, **kwargs):
        raise NotImplementedError("Rows cannot be put to a composite directly!")