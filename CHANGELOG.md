# edfi_api_client v0.2.0
## New Features
- `EdFiClient.get_swagger()` now returns an EdFiSwagger class that parses OpenAPI Swagger specification.
- `EdFiClient.resources` and `EdFiClient.descriptors` lazily retrieves lists of respective endpoints from Swagger.
- `EdFiEndpoint` child class attributes `description` and `has_deletes` lazily retrieves this metadata from Swagger.

# edfi_api_client v0.1.1
## Fixes
- Retry on 500 errors

# edfi_api_client v0.1.0
Initial release