from collections import defaultdict
from typing import Dict, List, Optional

from edfi_api_client import util


class EdFiEndpointMetadata:
    def __init__(self, namespace: str, endpoint: str):
        self.namespace: str = namespace
        self.endpoint : str = endpoint

        self.description: Optional[str] = None
        self.has_deletes: bool = False
        self.required_fields: List[str] = []
        self.fields: List[str] = []

    def __repr__(self):
        return "<EdFiEndpointMetadata: {}; {} fields ({} required)>" \
                .format(self.tuple, len(self.fields), len(self.required_fields))

    @property
    def tuple(self):
        return self.namespace, self.endpoint

    @property
    def definition_id(self) -> str:
        """
        Ed-Fi definitions use "edFi_students" convention, instead of standard "ed-fi/students".
        """
        ns = util.snake_to_camel(self.namespace)
        ep = util.plural_to_singular(self.endpoint)
        return f"{ns}_{ep}"


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

        # Extract resource descriptions from `tags`
        self.descriptions: dict = self.get_descriptions()

        # Extract surrogate keys from `definitions`
        self.reference_skeys: dict = self.get_reference_skeys(exclude=['link',])

        # Extract namespaces and endpoints, and whether there is a deletes endpoint from `paths`
        self.endpoints_meta: List[EdFiEndpointMetadata] = self.get_endpoints_meta()
        self.endpoints: List[(str, str)] = [meta.tuple for meta in self.endpoints_meta]
        self.deletes: List[(str, str)] = [meta.tuple for meta in self.endpoints_meta if meta.has_deletes]

    def __repr__(self):
        """
        Ed-Fi {self.type} OpenAPI Swagger Specification
        """
        return f"<Ed-Fi {self.type.title()} OpenAPI Swagger Specification>"

    def get_endpoints_meta(self):
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
        deletes: Dict[(str, str), bool] = defaultdict(bool)

        for path in self.json.get('paths', {}).keys():
            namespace = path.split('/')[1]
            endpoint  = path.split('/')[2]

            deletes[(namespace, endpoint)] |= ('/deletes' in path)

        # Re-iterate the found endpoints and deletes to build metadata.
        endpoints_meta: List[EdFiEndpointMetadata] = []

        for (namespace, endpoint), has_deletes in deletes.items():
            meta = EdFiEndpointMetadata(namespace, endpoint)
            meta.has_deletes = has_deletes
            meta.description = self.descriptions.get(meta.endpoint)

            definition_attrs = self.json.get('definitions').get(meta.definition_id)
            if definition_attrs:
                meta.required_fields = definition_attrs.get('required')
                meta.fields = list(definition_attrs.get('properties', {}).keys()) or None

            endpoints_meta.append(meta)

        return endpoints_meta

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
