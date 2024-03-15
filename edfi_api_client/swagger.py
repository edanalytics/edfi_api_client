import logging
import requests

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from edfi_api_client import util


class EdFiSwagger:
    """
    """
    def __init__(self, base_url: str, component: str):
        """
        `self.json` is  initialized lazily when an attribute is called.

        :param base_url:
        :param component: Type of swagger payload passed (i.e., 'resources' or 'descriptors')
        :return:
        """
        self.base_url: str = base_url
        self.component: str = component

        # All attributes are retrieved from lazy payload dict
        self._json: Optional[dict] = None
        self._definitions: Optional[dict] = None

    def __repr__(self) -> str:
        """
        Ed-Fi {self.component} OpenAPI Swagger Specification
        """
        return f"<Ed-Fi {self.component.title()} OpenAPI Swagger Specification>"

    @property
    def json(self) -> dict:
        if not self._json:
            self._json = self.get_json()
        return self._json

    def get_json(self) -> dict:
        """
        OpenAPI Specification describes the entire Ed-Fi API surface in a
        JSON payload.
        Can be used to surface available endpoints.

        :return: Swagger specification definition, as a dictionary.
        """
        logging.info(f"[Get {self.component.title()} Swagger] Retrieving Swagger into memory...")
        swagger_url = util.url_join(
            self.base_url, 'metadata', 'data/v3', self.component, 'swagger.json'
        )
        return requests.get(swagger_url).json()

    # Class attributes
    @property
    def version(self) -> Optional[str]:
        return self.json.get('swagger')

    @property
    def version_url_string(self) -> Optional[str]:
        return self.json.get('basePath')

    @property
    def token_url(self) -> Optional[str]:
        return (
            self.json
                .get('securityDefinitions', {})
                .get('oauth2_client_credentials', {})
                .get('tokenUrl')
        )

    @property
    def definitions(self) -> Dict[str, Dict[str, str]]:
        # Only complete expensive parsing operation once.
        if self._definitions:
            return self._definitions

        self._definitions: Dict[str, Dict[str, str]] = defaultdict(dict)
        for definition_id, definition_metadata in self.json.get('definitions', {}).items():

            # Add universal keys to the definition mapping.
            self._definitions[definition_id].update({
                'field_dtypes': {},
                'identity': [],
                'references': set(),
                'required': definition_metadata.get('required', []),
            })

            for field, field_metadata in definition_metadata.get('properties', {}).items():

                # References must be revisited a second time after all definitions have been parsed.
                # e.g.: {"$ref": "#/definitions/edFi_educationOrganizationReference"}
                if '$ref' in field_metadata:
                    reference_name = field_metadata['$ref'].split('/')[-1]
                    if reference_name == "link":
                        continue  # Ignore links

                    self._definitions[definition_id]['references'].add(reference_name)
                    self._definitions[definition_id]['field_dtypes'].update({field: reference_name})

                else:
                    # Default to most explicit datatype format.
                    dtype = field_metadata.get('format', field_metadata.get('type'))
                    self._definitions[definition_id]['field_dtypes'].update({field: dtype})

                if field_metadata.get('x-Ed-Fi-isIdentity'):
                    self._definitions[definition_id]['identity'].append(field)

        # Second pass to resolve references
        for definition_id, definition_metadata in self._definitions.items():
            field_dtype_extensions = {}

            for field, dtype in definition_metadata.get('field_dtypes', {}).items():
                if dtype in definition_metadata.get('references', []):
                    self._definitions[definition_id]['field_dtypes'].update({field: self._definitions.get(dtype)})

                    # Update array fields with nested information
                    for subfield, subfield_dtype in self._definitions[definition_id]['field_dtypes'][field]['field_dtypes'].items():
                        field_dtype_extensions.update({f"{field}.{subfield}": subfield_dtype})
                    for subfield in self._definitions[definition_id]['field_dtypes'][field]['identity']:
                        self._definitions[definition_id]['identity'].append(f"{field}.{subfield}")
                    for subfield in self._definitions[definition_id]['field_dtypes'][field]['required']:
                        self._definitions[definition_id]['required'].append(f"{field}.{subfield}")

            # Apply extensions after iterating to prevent "dictionary changed size during iteration"
            self._definitions[definition_id]['field_dtypes'].update(field_dtype_extensions)

            # Force definitions into an array, instead of the original set.
            self._definitions[definition_id]['references'] = list(self._definitions[definition_id]['references'])

        return self._definitions


    ### Endpoint Metadata Methods
    @staticmethod
    def build_definition_id(namespace: str, endpoint: str) -> str:
        """
        Ed-Fi definitions use "edFi_students" convention, instead of standard "ed-fi/students".
        """
        ns = util.snake_to_camel(namespace)
        ep = util.plural_to_singular(endpoint)
        return f"{ns}_{ep}"

    def get_endpoints(self) -> List[str]:
        return list(self.get_endpoint_deletes().keys())

    def get_endpoint_deletes(self) -> Dict[Tuple[str, str], bool]:
        """
        Internal function to parse values in `paths` and retrieve a list of metadata.

        Extract each Ed-Fi namespace and resource, and whether it has an optional deletes tag.
            (namespace: str, resource: str) -> has_deletes: bool

        Swagger's `paths` is a dictionary of Ed-Fi pathing keys (up-to-three keys per resource/descriptor).
        For example:
            '/ed-fi/studentSchoolAssociations'
            '/ed-fi/studentSchoolAssociations/{id}'
            '/ed-fi/studentSchoolAssociations/deletes'

        :return:
        """
        # Build out a collection of endpoints and their delete statuses by path.
        path_delete_mapping: Dict[(str, str), bool] = defaultdict(bool)

        for path in self.json.get('paths', {}).keys():
            namespace = path.split('/')[1]
            endpoint = path.split('/')[2]
            path_delete_mapping[(namespace, endpoint)] |= ('/deletes' in path)

        return path_delete_mapping

    def get_endpoint_fields(self, exclude: List[str] = ('id', '_etag')) -> Dict[Tuple[str, str], List[str]]:
        """
        Return
        """
        endpoint_field_dtypes = self.get_endpoint_field_dtypes(exclude)
        return {
            endpoint: list(field_dtypes.keys()) for endpoint, field_dtypes in endpoint_field_dtypes.items()
        }

    def get_endpoint_field_dtypes(self, exclude: List[str] = ('id', '_etag')) -> Dict[Tuple[str, str], Dict[str, str]]:
        """

        :param exclude:
        :return:
        """
        field_mapping: Dict[Tuple[str, str], Union[List[str], Dict[str, str]]] = defaultdict(dict)

        for namespace, endpoint in self.get_endpoints():
            endpoint_definition_id = self.build_definition_id(namespace, endpoint)

            for definition_id, metadata in self.definitions.items():
                if definition_id != endpoint_definition_id:
                    continue

                for field, dtype in metadata['field_dtypes'].items():
                    if field in exclude:
                        continue
                    field_mapping[(namespace, endpoint)].update({field: dtype})

        return field_mapping

    def get_endpoint_required_fields(self) -> Dict[Tuple[str, str], List[str]]:
        """

        :return:
        """
        field_mapping: Dict[Tuple[str, str], List[str]] = {}

        for namespace, endpoint in self.get_endpoint_deletes().keys():
            endpoint_definition_id = self.build_definition_id(namespace, endpoint)

            for definition_id, metadata in self.json.get('definitions').items():
                if definition_id == endpoint_definition_id:
                    field_mapping[(namespace, endpoint)] = list(metadata.get('required', []))

        return field_mapping

    def get_endpoint_identity_fields(self) -> Dict[Tuple[str, str], List[str]]:
        """

        :return:
        """
        field_mapping: Dict[Tuple[str, str], List[str]] = {}

        for namespace, endpoint in self.get_endpoints():
            endpoint_definition_id = self.build_definition_id(namespace, endpoint)

            for definition_id, metadata in self.definitions.items():
                if definition_id != endpoint_definition_id:
                    continue

                field_mapping[(namespace, endpoint)] = metadata['identity']

        return field_mapping

    def get_reference_skeys(self, exclude: List[str] = ('link', )) -> Dict[str, List[str]]:
        """
        Build surrogate key definition column mappings for each Ed-Fi reference.

        :return:
        """
        skey_mapping: Dict[str, List[str]] = {}

        for key, definition in self.json.get('definitions', {}).items():

            # Only reference surrogate keys are used
            if not key.endswith('Reference'):
                continue

            reference = key.split('_')[1]  # e.g.`edFi_staffReference`

            columns = definition.get('properties', {}).keys()
            columns = list(filter(lambda x: x not in exclude, columns))  # Remove columns to be excluded.

            skey_mapping[reference] = columns

        return skey_mapping

    def get_endpoint_descriptions(self) -> Dict[str, str]:
        """
        Descriptions for all EdFi endpoints are found under `tags` as [name, description] JSON objects.
        Their extraction is optional for YAML templates, but they look nice.

        :return:
        """
        return {
            tag['name']: tag['description']
            for tag in self.json['tags']
        }
