from collections import defaultdict
from typing import List

from edfi_api_client.util import camel_to_snake


class EdFiSwagger:
    """
    """
    def __init__(self, component: str, swagger_payload: dict):
        """
        TODO: Can `component` be extracted from the swagger?

        :param component: Type of swagger payload passed (i.e., 'resources' or 'descriptors')
        :param swagger_payload:
        :return:
        """
        self.type: str  = component
        self.json: dict = swagger_payload

        self.version: str = self.json.get('swagger')
        self.version_url_string: str = self.json.get('basePath')

        self.token_url: str = (
            self.json
                .get('securityDefinitions', {})
                .get('oauth2_client_credentials', {})
                .get('tokenUrl')
        )

        # Extract namespaces and endpoints, and whether there is a deletes endpoint from `paths`
        _endpoint_deletes = self._get_namespaced_endpoints_and_deletes()
        self.endpoints: list = list(_endpoint_deletes.keys())
        self.deletes  : list = list(filter(_endpoint_deletes.get, _endpoint_deletes))  # Filter where values are True

        # Extract resource descriptions from `tags`
        self.descriptions: dict = self.get_descriptions()

        # Extract surrogate keys from `definitions`
        self.reference_skeys: dict = self.get_reference_skeys(exclude=['link',])


    def __repr__(self):
        """
        Ed-Fi {self.type} OpenAPI Swagger Specification
        """
        return f"<Ed-Fi {self.type.title()} OpenAPI Swagger Specification>"


    def _get_namespaced_endpoints_and_deletes(self):
        """
        Internal function to parse values in `paths`.

        Extract each Ed-Fi namespace and resource, and whether it has an optional deletes tag.
            (namespace: str, resource: str) -> has_deletes: bool

        Swagger's `paths` is a dictionary of Ed-Fi pathing keys (up-to-three keys per resource/descriptor).
        For example:
            '/ed-fi/studentSchoolAssociations'
            '/ed-fi/studentSchoolAssociations/{id}'
            '/ed-fi/studentSchoolAssociations/deletes'

        :return:
        """
        resource_deletes = defaultdict(bool)

        for path in self.json.get('paths', {}).keys():
            namespace = path.split('/')[1]
            resource  = path.split('/')[2]
            has_deletes = ('/deletes' in path)

            resource_deletes[ (namespace, resource) ] |= has_deletes

        return resource_deletes


    def get_descriptions(self):
        """
        Descriptions for all EdFi endpoints are found under `tags` as [name, description] JSON objects.
        Their extraction is optional for YAML templates, but they look nice.

        :param swagger: Swagger JSON object
        :return:
        """
        return {
            tag['name']: tag['description']
            for tag in self.json['tags']
        }


    def get_reference_skeys(self, exclude: List[str]):
        """
        Build surrogate key definition column mappings for each Ed-Fi reference.

        :return:
        """
        skey_mapping = {}

        for key, definition in self.json.get('definitions', {}).items():

            # Only reference surrogate keys are used
            if not key.endswith('Reference'):
                continue

            reference = key.split('_')[1]  # e.g.`edFi_staffReference`

            columns = definition.get('properties', {}).keys()
            columns = list(filter(lambda x: x not in exclude, columns))  # Remove columns to be excluded.

            skey_mapping[reference] = columns

        return skey_mapping
