import jsonref
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
    def definitions(self) -> None:
        """
        Example definition:

        ```
        "definition1": {
            "properties": {
                "field1": {
                    "description": "",
                    "type": "string"
                },
                "field2": {
                    "$ref": "#/definitions/definition2"
                },
                "field3": {
                    "description": "",
                    "items": {
                        "$ref": "#/definitions/definition3"
                    },
                    "type": "array"
                }
            },
            "required": [
                "field1"
            ],
            "type": "object"
        }
        ```
        """
        # Only complete reference-resolution once.
        if not self._definitions:

            # Definitions were renamed in 7.1.
            # Extract definitions into a new object to only resolve definition references.
            if 'components' in self.json:
                raw_definitions = self.json.get('components', {}).get('schemas', {})
                wrapper_json = {"components": {"schemas": raw_definitions}}
            else:
                raw_definitions = self.json.get('definitions', {})
                wrapper_json = {"definitions": raw_definitions}

            if not raw_definitions:
                raise KeyError("No definitions found in Swagger JSON!")

            # Resolve references before returning JSON.
            resolved = jsonref.replace_refs(wrapper_json, proxies=False, lazy_load=False)

            # Re-extract the definitions before returning.
            if 'components' in resolved:
                self._definitions = resolved['components']['schemas']
            else:
                self._definitions = resolved['definitions']

        return self._definitions


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
