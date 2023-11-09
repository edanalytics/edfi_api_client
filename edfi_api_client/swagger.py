from collections import defaultdict
from typing import Dict, List, Tuple

from edfi_api_client import util


class EdFiSwagger:
    """
    """
    def __init__(self, component: str, swagger_payload: dict):
        """

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
        self.endpoints: List[(str, str)] = list(self.get_path_deletes().keys())
        self.deletes: List[(str, str)] = [endpoint for endpoint, has_deletes in self.get_path_deletes().items() if has_deletes]

        # Extract fields and surrogate keys from `definitions`
        self.endpoint_fields: Dict[(str, str), List[str]] = self.get_fields(exclude=['id', '_etag'])
        self.endpoint_required_fields: Dict[(str, str), List[str]] = self.get_required_fields()
        self.reference_skeys: Dict[str, List[str]] = self.get_reference_skeys(exclude=['link', ])

        # Extract resource descriptions from `tags`
        self.descriptions: Dict[str, str] = self.get_descriptions()

    def __repr__(self):
        """
        Ed-Fi {self.type} OpenAPI Swagger Specification
        """
        return f"<Ed-Fi {self.type.title()} OpenAPI Swagger Specification>"


    # PATHS
    def get_path_deletes(self):
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
            endpoint  = path.split('/')[2]

            path_delete_mapping[(namespace, endpoint)] |= ('/deletes' in path)

        return path_delete_mapping


    # DEFINITIONS
    @staticmethod
    def build_definition_id(namespace: str, endpoint: str) -> str:
        """
        Ed-Fi definitions use "edFi_students" convention, instead of standard "ed-fi/students".
        """
        ns = util.snake_to_camel(namespace)
        ep = util.plural_to_singular(endpoint)
        return f"{ns}_{ep}"

    def get_fields(self, exclude: List[str] = ()) -> Dict[Tuple[str, str], List[str]]:
        """

        :param exclude:
        :return:
        """
        field_mapping: Dict[Tuple[str, str], List[str]] = {}

        for definition_id, metadata in self.json.get('definitions').items():
            for namespace, endpoint in self.endpoints:

                if self.build_definition_id(namespace, endpoint) == definition_id:
                    filtered_fields = [field for field in metadata.get('properties', {}).keys() if field not in exclude]
                    field_mapping[(namespace, endpoint)] = filtered_fields

        return field_mapping

    def get_required_fields(self) -> Dict[Tuple[str, str], List[str]]:
        """

        :return:
        """
        field_mapping: Dict[Tuple[str, str], List[str]] = {}

        for definition_id, metadata in self.json.get('definitions').items():
            for namespace, endpoint in self.endpoints:

                if self.build_definition_id(namespace, endpoint) == definition_id:
                    field_mapping[(namespace, endpoint)] = list(metadata.get('required', []))

        return field_mapping

    def get_reference_skeys(self, exclude: List[str]):
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


    # TAGS
    def get_descriptions(self):
        """
        Descriptions for all EdFi endpoints are found under `tags` as [name, description] JSON objects.
        Their extraction is optional for YAML templates, but they look nice.

        :return:
        """
        return {
            tag['name']: tag['description']
            for tag in self.json['tags']
        }
