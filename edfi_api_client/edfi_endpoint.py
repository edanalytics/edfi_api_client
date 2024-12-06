import logging
import jsonschema
import os
import requests

from requests import HTTPError

from edfi_api_client import util
from edfi_api_client.edfi_params import EdFiParams

from typing import Dict, Iterator, List, Optional, Tuple, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.edfi_client import EdFiClient
    from edfi_api_client.edfi_swagger import EdFiSwagger


class EdFiEndpoint:
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
        get_key_changes: bool = False,
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
        self.params = EdFiParams(params, **kwargs)

        # GET-specific deletes and keyChanges endpoints
        self.get_deletes: bool = get_deletes
        self.get_key_changes: bool = get_key_changes
        if self.get_deletes and self.get_key_changes:
            raise ValueError("Endpoint arguments `get_deletes` and `get_key_changes` are mutually-exclusive.")

        # Optional helper classes with lazy attributes
        self.client: 'EdFiClient' = client
        self.swagger: 'EdFiSwagger' = swagger
        self.validator: 'Draft4Validator' = None


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
        # Deletes/keyChanges are an optional path addition.
        if self.get_deletes:
            path_extra = 'deletes'
        elif self.get_key_changes:
            path_extra = 'keyChanges'
        else:
            path_extra = None

        return util.url_join(self._endpoint_url, self.namespace, self.name, path_extra)


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
    
    def validate(self, payload: dict):
        """
        Validate a payload against the expected endpoint structure, as outlined in its Swagger definition.
        """
        # Create the validator only once to improve performance when validating many rows.
        if not self.validator:
            self.validator = jsonschema.Draft4Validator(self.definition)
        self.validator.validate(payload)

    @property
    def fields(self) -> List[str]:
        return list(self.field_dtypes.keys())

    @property
    def field_dtypes(self) -> Dict[str, str]:
        return self._recurse_definition_schema(self.definition)['field_dtypes']
        
    @property
    def required_fields(self) -> List[str]:
        return list(self._recurse_definition_schema(self.definition)['required'])

    @property
    def identity_fields(self) -> List[str]:
        return list(self._recurse_definition_schema(self.definition)['identity'])

    @property
    def has_deletes(self) -> bool:
        return self.swagger.get_endpoint_deletes().get((self.namespace, self.name))

    @property
    def description(self) -> Optional[str]:
        return self.swagger.get_endpoint_descriptions().get(self.name)
    
    def _recurse_definition_schema(self,
        schema: dict,
        parent_field: Optional[str] = None,
        collections: Optional[dict] = None
    ) -> dict:
        """
        Recurse a definition JSON schema and extract metadata to display to user.
        Note: Parents are always included with their children.
        """
        # Set collections object in top-level call.
        if not collections:
            collections = {
                "field_dtypes": dict(),
                "identity": set(),
                "required": set(),
            }

        # Optional parent FIELD_DTYPE
        if parent_field:
            collections['field_dtypes'][parent_field] = schema.get('format', schema.get('type'))

        # REQUIRED_FIELDS
        for field in schema.get('required', []):
            if parent_field:
                collections['required'].update([parent_field, f"{parent_field}.{field}"])
            else:
                collections['required'].add(field)

        # Recurse option 1: Arrays
        # Arrays MUST be nested fields, so parent_field is always defined.
        if 'items' in schema:
            collections = self._recurse_definition_schema(schema['items'], parent_field, collections)

        # Recurse option 2: Fields
        for field, metadata in schema.get('properties', {}).items():
            full_field = f"{parent_field}.{field}" if parent_field else field

            # FIELD_DTYPES
            collections['field_dtypes'][full_field] = metadata.get('format', metadata.get('type'))
            
            # IDENTITY_FIELDS
            if metadata.get('x-Ed-Fi-isIdentity'):
                if parent_field:
                    collections['identity'].update([parent_field, f"{parent_field}.{field}"])
                else:
                    collections['identity'].add(field)

            collections = self._recurse_definition_schema(metadata, full_field, collections)

        return collections


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
        # If a filter is applied, the URL changes to match the filter type.
        if self.filter_type is None and self.filter_id is None:
            return util.url_join(self._endpoint_url, self.namespace, self.composite, self.name.title())

        elif self.filter_type is not None and self.filter_id is not None:
            return util.url_join(
                self._endpoint_url, self.namespace, self.composite,
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