import logging
import os
import requests

from collections import defaultdict

from edfi_api_client import util
from edfi_api_client.async_mixin import AsyncEndpointMixin
from edfi_api_client.params import EdFiParams

from typing import BinaryIO, Dict, Iterator, List, Optional, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.client import EdFiClient


class EdFiEndpoint(AsyncEndpointMixin):
    """
    This is an abstract class for interacting with Ed-Fi resources and descriptors.
    Composites override EdFiEndpoint with custom composite-logic.
    """
    type: str = None
    swagger_type: str = None

    def __init__(self,
         client: 'EdFiClient',
         name: str,

         *,
         namespace: str = 'ed-fi',
         get_deletes: bool = False,
         params: Optional[dict] = None,
         **kwargs
    ):
        self.client: 'EdFiClient' = client

        # Name and namespace can be passed manually
        if isinstance(name, str):
            self.name: str = util.snake_to_camel(name)
            self.namespace: str = namespace

        # Or as a `(namespace, name)` tuple as output from Swagger
        else:
            try:
                self.namespace, self.name = name
            except ValueError:
                logging.error(
                    "Arguments `name` and `namespace` must be passed explicitly, or as a `(namespace, name)` tuple."
                )

        # Build URL and dynamic params object
        self.get_deletes: bool = get_deletes
        self.params = EdFiParams(params, **kwargs)

        # Swagger attributes are loaded lazily
        self.swagger = self.client.swaggers.get(self.swagger_type)

    def __repr__(self):
        """
        Endpoint (Deletes) (with {N} parameters) [{namespace}/{name}]
        """
        deletes_string = " Deletes" if self.get_deletes else ""
        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        return f"<{self.type}{deletes_string}{params_string} [{self.raw}]>"

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
            self.client.base_url,
            self.client.version_url_string,
            self.client.instance_locator,
            self.namespace, self.name, deletes
        )


    ### Generic API methods
    def ping(self, **kwargs) -> requests.Response:
        """
        This method pings the endpoint and verifies it is accessible.

        :return:
        """
        params = self.params.copy()
        params['limit'] = 1

        res = self.client.session.get_response(self.url, params=params, **kwargs)

        # To ping a composite, a limit of at least one is required.
        # We do not want to surface student-level data during ODS-checks.
        if res.ok:
            res._content = b'{"message": "Ping was successful! ODS data has been intentionally scrubbed from this response."}'

        return res

    def total_count(self, **kwargs):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        return self.client.session.get_total_count(self.url, self.params, **kwargs)

    def get(self, limit: Optional[int] = None, **kwargs) -> List[dict]:
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        self.client.verbose_log(f"[Get {self.type}] Endpoint  : {self.url}")
        self.client.verbose_log(f"[Get {self.type}] Parameters: {self.params}")

        params = self.params.copy()

        if limit is not None:
            params['limit'] = limit

        return self.client.session.get_response(self.url, params=params, **kwargs).json()


    ### Swagger-adjacent properties and helper methods
    def get_swagger_if_none(self):
        """
        Gets the endpoint's Swagger if not already collected.
        :return:
        """
        if self.swagger is None:
            self.swagger = self.client.get_swagger(self.swagger_type)  # Updates client.swaggers

    @property
    def description(self) -> str:
        self.get_swagger_if_none()
        return self.swagger.descriptions.get(self.name)

    @property
    def has_deletes(self) -> bool:
        self.get_swagger_if_none()
        return (self.namespace, self.name) in self.swagger.deletes

    @property
    def fields(self) -> List[str]:
        self.get_swagger_if_none()
        return self.swagger.endpoint_fields.get((self.namespace, self.name))

    @property
    def required_fields(self) -> List[str]:
        self.get_swagger_if_none()
        return self.swagger.endpoint_required_fields.get((self.namespace, self.name))


    ### GET Methods
    def get_pages(self,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a generator.

        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        self.client.verbose_log(f"[Paged Get {self.type}] Endpoint  : {self.url}")

        if step_change_version and reverse_paging:
            self.client.verbose_log(f"[Paged Get {self.type}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
        elif step_change_version:
            self.client.verbose_log(f"[Paged Get {self.type}] Pagination Method: Change Version Stepping")
        else:
            self.client.verbose_log(f"[Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Build a list of pagination params to iterate during ingestion.
        paged_params_list = self.get_paged_window_params(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        # Begin pagination-loop
        for paged_params in paged_params_list:
            self.client.verbose_log(f"[Paged Get {self.type}] Parameters: {paged_params}")
            res = self.client.session.get_response(self.url, params=paged_params, **kwargs)

            self.client.verbose_log(f"[Paged Get {self.type}] Retrieved {len(res.json())} rows.")
            yield res.json()

    def get_rows(self,
        *,
        page_size: int = 100,
        reverse_paging: bool = True,
        step_change_version: bool = False,
        change_version_step_size: int = 50000,
        **kwargs
    ) -> Iterator[dict]:
        """
        This method returns all rows from an endpoint, applying pagination logic as necessary.
        Rows are returned as a generator.

        :param page_size:
        :param reverse_paging:
        :param step_change_version:
        :param change_version_step_size:
        :return:
        """
        paged_result_iter = self.get_pages(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        for paged_result in paged_result_iter:
            yield from paged_result

    def get_to_json(self,
        path: str,

        *,
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
        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        self.client.verbose_log(f"Writing rows to disk: `{path}`")

        paged_results = self.get_pages(
            page_size=page_size, reverse_paging=reverse_paging,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        with open(path, 'wb') as fp:
            for page in paged_results:
                fp.write(util.page_to_bytes(page))

        return path

    def get_paged_window_params(self,
        *,
        page_size: int,
        reverse_paging: bool,
        step_change_version: bool,
        change_version_step_size: int,
        **kwargs
    ) -> Iterator[EdFiParams]:
        """

        :param page_size:
        :param step_change_version:
        :param change_version_step_size:
        :param reverse_paging:
        :return:
        """
        if step_change_version:
            for cv_window_params in self.params.build_change_version_window_params(change_version_step_size):
                total_count = self.client.session.get_total_count(self.url, cv_window_params, **kwargs)

                cv_offset_params_list = cv_window_params.build_offset_window_params(page_size, total_count=total_count, reverse=reverse_paging)
                yield from cv_offset_params_list

        else:
            total_count = self.client.session.get_total_count(self.url, self.params, **kwargs)
            yield from self.params.build_offset_window_params(page_size, total_count=total_count)


    ### POST Methods
    def post_rows(self,
        rows: Union[Iterator[dict], BinaryIO],
        *,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Dict[str, List[int]]:
        """
        This method tries to post all rows from an iterator.

        :param rows:
        :param include:
        :param exclude:
        :return:
        """
        self.client.verbose_log(f"[Post {self.type}] Endpoint  : {self.url}")
        output_log = defaultdict(list)

        for idx, row in enumerate(rows):

            if include and idx not in include:
                continue
            elif exclude and idx in exclude:
                continue

            try:
                response = self.client.session.post_response(self.url, data=row, **kwargs)

                if response.ok:
                    output_log[f"{response.status_code}"].append(idx)
                else:
                    output_log[f"{response.status_code} {response.json().get('message')}"].append(idx)

            except Exception as error:
                output_log[str(error)].append(idx)

        return dict(output_log)

    def post_from_json(self,
        path: str,
        *,
        include: Iterator[int] = None,
        exclude: Iterator[int] = None,
        **kwargs
    ) -> Dict[str, List[int]]:
        """

        :param path:
        :param include:
        :param exclude:
        :return:
        """
        self.client.verbose_log(f"Posting rows from disk: `{path}`")

        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")

        with open(path, 'rb') as fp:
            return self.post_rows(fp, include=include, exclude=exclude, **kwargs)


    ### DELETE Methods
    def delete_ids(self, ids: Iterator[int], **kwargs):
        """
        Delete all records at the endpoint by ID.

        :param ids:
        :return:
        """
        self.client.verbose_log(f"[Delete {self.type}] Endpoint  : {self.url}")

        for id in ids:
            self.client.session.delete_response(self.url, id=id, **kwargs)


class EdFiResource(EdFiEndpoint):
    """
    Ed-Fi Resources are the primary use-case of the API.
    """
    type: str = 'Resource'
    swagger_type: str = 'resources'


class EdFiDescriptor(EdFiEndpoint):
    """
    Ed-Fi Descriptors are used identically to Resources, but they are listed in a separate Swagger.
    """
    type: str = 'Descriptor'
    swagger_type: str = 'descriptors'


class EdFiComposite(EdFiEndpoint):
    """

    """
    type: str = 'Composite'
    swagger_type: str = 'composites'

    def __init__(self,
        client: 'EdFiClient',
        name: str,

        *,
        namespace: str = 'ed-fi',
        composite: str = 'enrollment',
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,

        params: Optional[dict] = None,
        **kwargs
    ):
        # Assign composite-specific arguments that are used in `self.url()`.
        self.composite: str = composite
        self.filter_type: Optional[str] = filter_type
        self.filter_id: Optional[str] = filter_id

        super().__init__(client=client, name=name, namespace=namespace, params=params)

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
                self.client.base_url, 'composites/v1',
                self.client.instance_locator,
                self.namespace, self.composite, self.name.title()
            )
        elif self.filter_type is not None and self.filter_id is not None:
            return util.url_join(
                self.client.base_url, 'composites/v1',
                self.client.instance_locator,
                self.namespace, self.composite,
                self.filter_type, self.filter_id, self.name
            )
        else:
            raise ValueError(
                "`filter_type` and `filter_id` must both be specified if a filter is being applied!"
            )

    def total_count(self):
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        raise NotImplementedError(
            "Total counts have not yet been implemented in Ed-Fi composites!"
        )

    def get_pages(self, *, page_size: int = 100, **kwargs) -> Iterator[List[dict]]:
        """
        This method completes a series of GET requests, paginating params as necessary based on endpoint.
        Rows are returned as a generator.

        :param page_size:
        :return:
        """
        if kwargs.get('step_change_version'):
            raise KeyError(
                "Change versions are not implemented in composites! Remove `step_change_version` from arguments."
            )

        self.client.verbose_log(f"[Paged Get {self.type}] Endpoint  : {self.url}")
        self.client.verbose_log(f"[Paged Get {self.type}] Pagination Method: Offset Pagination")

        # Reset pagination parameters
        paged_params = self.params.copy()
        paged_params['limit'] = page_size
        paged_params['offset'] = 0

        # Begin pagination-loop
        while True:

            ### GET from the API and yield the resulting JSON payload
            self.client.verbose_log(f"[Paged Get {self.type}] Parameters: {paged_params}")
            res = self.client.session.get_response(self.url, params=paged_params, **kwargs)

            # If rows have been returned, there may be more to ingest.
            if res.json():
                self.client.verbose_log(f"[Paged Get {self.type}] Retrieved {len(res.json())} rows.")
                yield res.json()

                self.client.verbose_log(f"@ Paginating offset...")
                paged_params['offset'] += page_size

            # If no rows are returned, end pagination.
            else:
                self.client.verbose_log(f"[Paged Get {self.type}] @ Retrieved zero rows. Ending pagination.")
                break

    def post_rows(self, *args, **kwargs):
        raise NotImplementedError(
            "Rows cannot be posted to a composite directly!"
        )
