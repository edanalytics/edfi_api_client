# edfi_api_client v0.2.2
## New Features
- Access resource `/keyChanges` endpoint using optional `get_key_changes` flag in `EdFiResource`.
- `EdFiClient.get_token_info()` returns the JSON payload of the `/oauth/token_info` endpoint for the existing connection's access-token.

## Fixes
- Fix authentication logic and require `instance_code` be passed when authenticating to `instance_year_specific` ODSes.


# edfi_api_client v0.2.1
## Fixes
- Make `api_mode` a fully-optional argument in `EdFiClient` (necessary for v7 ODSes).


# edfi_api_client v0.2.0
## New Features
- `EdFiClient.get_swagger()` now returns an EdFiSwagger class that parses OpenAPI Swagger specification.
- `EdFiClient.resources` and `EdFiClient.descriptors` lazily retrieves lists of respective endpoints from Swagger.
- `EdFiEndpoint` child class attributes `description` and `has_deletes` lazily retrieves this metadata from Swagger.

## Under the hood
- Requests re-authenticate automatically, based on the expiration-time retrieved from the API.


# edfi_api_client v0.1.4
## Fixes
- Compatibility fix for Ed-Fi 6.0: casing changed for change version API responses


# edfi_api_client v0.1.2
## New features
- New "reverse_paging" pagination method for `EdFiResource.get_pages()`

## Under the hood
- Default to reverse-paging when change-version stepping resources

## Fixes
- Fix bug in `EdFiResource.get_pages()` where default `change_version_step_size` was used instead of argument


# edfi_api_client v0.1.1
## Fixes
- Retry on 500 errors


# edfi_api_client v0.1.0
Initial release
