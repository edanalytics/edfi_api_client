import abc
import logging
import requests

from typing import Iterator, List, Optional, Tuple, Union

from edfi_api_client.edfi_params import EdFiParams
from edfi_api_client import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.edfi_client import EdFiClient

from joblib import Parallel, delayed 
from functools import partial


class EdFiEndpoint:
    """
    This is an abstract class for interacting with Ed-Fi resources and descriptors.
    Composites override with custom composite-logic.
    """
    component: str = None

    def __init__(self,
        client: 'EdFiClient',
        name: Union[str, Tuple[str, str]],
        namespace: str = 'ed-fi',
        get_deletes: bool = False,
        get_key_changes: bool = False,
        params: Optional[dict] = None,
        **kwargs
    ):
        self.client: 'EdFiClient' = client

        # Names can be passed manually or as a `(namespace, name)` tuple as output from Swagger.
        self.namespace, self.name = self._parse_names(self.client, namespace, name)
        self.params = EdFiParams(params, **kwargs)

        # GET-specific deletes and keyChanges endpoints
        self.get_deletes: bool = get_deletes
        self.get_key_changes: bool = get_key_changes
        if self.get_deletes and self.get_key_changes:
            raise ValueError("Endpoint arguments `get_deletes` and `get_key_changes` are mutually-exclusive.")

        # Optional helper classes with lazy attributes
        self.client: 'EdFiClient' = client
        # self.swagger: 'EdFiSwagger' = swagger
        # self.validator: 'Draft4Validator' = None

        self._description: Optional[str]  = None
        self._has_deletes: Optional[bool] = None


    def __repr__(self):
        """
        Endpoint (Deletes) (with {N} parameters) [{namespace}/{name}]
        """
        if self.get_deletes:
            extras_string = " Deletes"
        elif self.get_key_changes:
            extras_string = " KeyChanges"
        else:
            extras_string = ""

        params_string = f" with {len(self.params.keys())} parameters" if self.params else ""
        return f"<{self.component}{extras_string}{params_string} [{self.raw}]>"
    

    ### Naming and Pathing Methods
    @staticmethod
    def _parse_names(client: 'EdFiClient', namespace: str, name: str) -> Tuple[str, str]:
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
        Include namespacing and optional pathing.

        :return:
        """
        # Deletes/keyChanges are an optional path addition.
        if self.get_deletes:
            path_extra = 'deletes'
        elif self.get_key_changes:
            path_extra = 'keyChanges'
        else:
            path_extra = None

        return util.url_join(
            self.client.base_url, 'data/v3', self.client.instance_locator,
            self.namespace, self.name, path_extra
        )


    def ping(self, *, params: Optional[dict] = None, **kwargs) -> requests.Response:
        """
        This method pings the endpoint and verifies it is accessible.

        :return:
        """
        logging.info(f"[Ping {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = EdFiParams(params or self.params).copy()
        params['limit'] = 1  # To ping a composite, a limit of at least one is required.

        # We do not want to surface student-level data during ODS-checks.
        res = self.client.session.get_response(self.url, params=params, **kwargs)
        if res.ok:
            res._content = b'{"message": "Ping was successful! ODS data has been intentionally scrubbed from this response."}'

        return res


    def get(self, url: Optional[str] = None, limit: Optional[int] = None, *, params: Optional[dict] = None, **kwargs) -> List[dict]:
        """
        This method returns the rows from a single GET request using the exact params passed by the user.

        :return:
        """
        end_point = url or self.url
        logging.info(f"[Get {self.component}] Endpoint: {end_point}")

        # Override init params if passed
        params = EdFiParams(params or self.params).copy()
        if limit:  # Override limit if passed
            params['limit'] = limit

        logging.info(f"[Get {self.component}] Parameters: {params}")

        resp = self.client.session.get_response(end_point, params=params, **kwargs).json()
        return resp


    def get_rows(self,
        *,
        params: Optional[dict] = None,  # Optional alternative params
        page_size: int = 100,
        reverse_paging: bool = False,
        step_change_version: bool = False,
        partitioning: bool = False,
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
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :return:
        """
        paged_result_iter = self.get_pages(
            params=params,
            page_size=page_size, reverse_paging=reverse_paging, cursor_paging= cursor_paging, partitioning=partitioning,
            step_change_version=step_change_version, change_version_step_size=change_version_step_size,
            **kwargs
        )

        for paged_result in paged_result_iter:
            yield from paged_result

    
    def get_pages_offset(self,
        *,
        url: Optional[str] = None,
        params: Optional[dict] = None,  # Optional alternative params
        limit: Optional[int] = None,
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
        :param retry_on_failure:
        :param max_retries:
        :param max_wait:
        :return:
        """
        # Override init params if passed
        paged_params = EdFiParams(params or self.params).copy()
        end_point = url or self.url
        logging.info(f"[Get {self.component}] Endpoint: {end_point}")

        if limit:  # Override limit if passed
            paged_params['limit'] = limit

        ### Prepare pagination variables, depending on type of pagination being used
        if step_change_version and reverse_paging:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Change Version Stepping with Reverse-Offset Pagination")
            paged_params.init_page_by_change_version_step(change_version_step_size)
            total_count = self.get_total_count(params=paged_params, **kwargs)
            paged_params.init_reverse_page_by_offset(total_count, page_size)

        elif step_change_version:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Change Version Stepping")
            paged_params.init_page_by_offset(page_size)
            paged_params.init_page_by_change_version_step(change_version_step_size)         
        else:
            logging.info(f"[Paged Get {self.component}] Pagination Method: Offset Pagination")
            paged_params.init_page_by_offset(page_size)


        # Begin pagination-loop
        while True:
            logging.info(f"[Get {self.component}] Parameters: {paged_params}")

            ### GET from the API and yield the resulting JSON payload
            paged_rows = self.client.session.get_response(end_point, params=paged_params, **kwargs).json()
            yield paged_rows
            logging.info(f"[Get {self.component}] Retrieved {len(paged_rows)} rows.")

            ### Paginate, depending on the method specified in arguments
            # Reverse offset pagination is only applicable during change-version stepping.
            if step_change_version and reverse_paging:
                logging.info(f"[Paged Get {self.component}] @ Reverse-paginating offset...")
                try:
                    paged_params.reverse_page_by_offset()
                except StopIteration:
                    logging.info(f"[Paged Get {self.component}] @ Reverse-paginated into negatives. Stepping change version...")
                    try:
                        paged_params.page_by_change_version_step()  # This raises a StopIteration if max change version is exceeded.
                        total_count = self.get_total_count(params=paged_params, **kwargs)
                        paged_params.init_reverse_page_by_offset(total_count, page_size)
                    except StopIteration:
                        logging.info(f"[Paged Get {self.component}] @ Change version exceeded max. Ending pagination.")
                        break
                
            else:
                # If no rows are returned, end pagination.
                if len(paged_rows) == 0:

                    if step_change_version:
                        try:
                            logging.info(f"[Paged Get {self.component}] @ Stepping change version...")
                            paged_params.page_by_change_version_step()  # This raises a StopIteration if max change version is exceeded.
                        except StopIteration:
                            logging.info(f"[Paged Get {self.component}] @ Change version exceeded max. Ending pagination.")
                            break
                    else:
                        logging.info(f"[Paged Get {self.component}] @ Retrieved zero rows. Ending pagination.")
                        break

                # Otherwise, paginate offset.
                else:
                    logging.info(f"@ Paginating offset...")
                    paged_params.page_by_offset()
    
    def get_pages_cursor(self,
        *,
        url: Optional[str] = None,
        params: Optional[dict] = None,  # Optional alternative params
        limit: Optional[int] = None,
        page_size: int = 100,
        **kwargs
    ) -> Iterator[List[dict]]:
        
        # Override init params if passed
        paged_params = EdFiParams(params or self.params).copy()
        end_point = url or self.url
        logging.info(f"[Get {self.component}] Endpoint: {end_point}")

        if limit:  # Override limit if passed
            paged_params['limit'] = limit

        ods_version = tuple(map(int, self.client.get_ods_version().split(".")[:2]))

        # Check ODS version compatibility for cursor paging
        if ods_version < (7,3):
            raise ValueError(f"ODS {self.client.get_ods_version()} is incompatible. Cursor Paging requires v.7.3 or higher. Ending pagination")

        # Raise error if User wants to retrieve deletes/keys with cursor paging
        if self.get_deletes or self.get_key_changes:
            raise ValueError(f"Cursor Paging does not support deletes/key_changes. Ending pagination")
        
        logging.info(f"[Paged Get {self.component}] Pagination Method: Cursor Paging")
        
        ###  Prepare pagination variables 
        ###  First request should not have any `page_token` and `page_size` defined
        paged_params.init_page_by_token(page_token = None, page_size = None)            
        
        # Begin pagination loop
        while True:
            logging.info(f"[Get {self.component}] Parameters: {paged_params}")

            result = self.client.session.get_response(end_point, params = paged_params, **kwargs)
            paged_rows = result.json()
            logging.info(f"[Get {self.component}] Retrieved {len(paged_rows)} rows")
            yield paged_rows
            
            logging.info(f"[Paged Get {self.component}] @ Cursor paging ...")
            if not result.headers.get("Next-Page-Token"):
                logging.info(f"[Paged Get {self.component}] @ Retrieved zero rows. Ending pagination.")
                break
            paged_params.init_page_by_token(page_token = result.headers.get("Next-Page-Token"), page_size = page_size)



    def get_total_count(self, *, params: Optional[dict] = None, **kwargs) -> int:
        """
        Ed-Fi 3 resources/descriptors can be fed an optional 'totalCount' parameter in GETs.
        This returns a 'Total-Count' in the response headers that gives the total number of rows for that resource with the specified params.
        Non-pagination params (i.e., offset and limit) have no impact on the returned total.

        :return:
        """
        logging.info(f"[Get Total Count {self.component}] Endpoint: {self.url}")

        # Override init params if passed
        params = EdFiParams(params or self.params).copy()
        params['totalCount'] = True
        params['limit'] = 0

        logging.info(f"[Get Total Count {self.component}] Parameters: {params}")
        res = self.client.session.get_response(self.url, params, **kwargs)
        return int(res.headers.get('Total-Count'))

    def total_count(self, *args, **kwargs) -> int:
        logging.warning("`EdFiEndpoint.total_count()` is deprecated. Use `EdFiEndpoint.get_total_count()` instead.")
        return self.get_total_count(*args, **kwargs)


    @property
    def description(self):
        if self._description is None:
            self._description = self._get_attributes_from_swagger()['description']
        return self._description

    @property
    def has_deletes(self):
        if self._has_deletes is None:
            self._has_deletes = self._get_attributes_from_swagger()['has_deletes']
        return self._has_deletes


    def _get_attributes_from_swagger(self):
        """
        Retrieve endpoint-metadata from the Swagger document.

        Populate the respective swagger object in `self.client` if not already populated.

        :return:
        """
        # "Resource" -> "resources"
        swagger_type = self.component.lower() + "s"

        # Only GET the Swagger if not already populated in the client.
        self.client._set_swagger(swagger_type)
        swagger = self.client.swaggers[swagger_type]

        # Populate the attributes found in the swagger.
        return {
            'description': swagger.descriptions.get(self.name),
            'has_deletes': (self.namespace, self.name) in swagger.deletes,
        }


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
        if self.get_key_changes:
            logging.warning("Composites do not have /keyChanges endpoints. Argument `get_key_changes` has been ignored.")


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
        base_composite_url = util.url_join(self.client.base_url, 'composites/v1', self.client.instance_locator)

        # If a filter is applied, the URL changes to match the filter type.
        if self.filter_type is None and self.filter_id is None:
            return util.url_join(base_composite_url, self.namespace, self.composite, self.name.title())

        elif self.filter_type is not None and self.filter_id is not None:
            return util.url_join(
                base_composite_url, self.namespace, self.composite,
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
        paged_params = EdFiParams(params or self.params).copy()
        paged_params.init_page_by_offset(page_size)

        # Begin pagination-loop
        while True:

            ### GET from the API and yield the resulting JSON payload
            res = self.get(params=paged_params)

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
