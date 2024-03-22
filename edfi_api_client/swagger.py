import json
import logging
import requests

from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Set, Union

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
        """
        Definitions are complex to parse due to nested references.
        Compile the full list of references before recursively populating them.
        """
        # Only complete expensive parsing operation once.
        if not self._definitions:
            # Warn if the definitions are missing from the Swagger.
            swagger_definitions = self.json.get('definitions', {})
            if not swagger_definitions:
                raise KeyError("No definitions found in Swagger JSON!")

            # First pass to build definitions.
            self._definitions: Dict[str, dict] = {
                definition_id: self._build_definition(json_definition)
                for definition_id, json_definition in swagger_definitions.items()
            }

            # Second pass to resolve references.
            for definition_id, definition in self._definitions.items():
                self._recurse_definition_references(definition)

        return self._definitions

    @staticmethod
    def _build_definition(json_definition: dict) -> dict:
        """
        Method to simplify definition parsing without needing a helper class.
        """
        definition = {
            "field_dtypes": defaultdict(str),
            "references": defaultdict(str),
            "identity": list(),
            "required": list(),
        }

        # Required keys are an optional top-level field
        if 'required' in json_definition:
            definition['required'].extend(json_definition['required'])

        # All other fields are parsed from properties
        for field, field_metadata in json_definition.get('properties', {}).items():

            # Record whether an identity field
            if field_metadata.get('x-Ed-Fi-isIdentity'):
                definition['identity'].append(field)

            # Use self.resolve_reference to flesh out reference in a second pass after all definitions are known.
            # Raw format: ``` {"$ref": "#/definitions/edFi_educationOrganizationReference"} ```
            if '$ref' in field_metadata:
                reference_name = field_metadata['$ref'].split('/')[-1]
                if reference_name == "link":
                    continue  # Ignore links
                definition['references'][field] = reference_name
            else:
                # Default to most explicit datatype format.
                dtype = field_metadata.get('format', field_metadata.get('type'))
                definition['field_dtypes'][field] = dtype

        return definition

    def _recurse_definition_references(self, definition: dict):
        """

        """
        for field, reference in definition['references'].items():
            # Recurse children before resolving parent.
            if isinstance(reference, str):
                reference = self.definitions[reference]
                self._recurse_definition_references(reference)
                definition['references'][field] = reference

            # Copy attributes up to parent definition.
            for subfield, dtype in reference['field_dtypes'].items():
                full_field = f"{field}.{subfield}"

                definition['field_dtypes'][full_field] = dtype
                if subfield in reference['identity']:
                    definition['identity'].append(full_field)
                if subfield in reference['required']:
                    definition['required'].append(full_field)


    ### Endpoint Metadata Methods
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
