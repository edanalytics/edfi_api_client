# edfi_api_client v1.0.0
## New Features
- `EdFiClient.get_swagger()` now returns an EdFiSwagger class that parses OpenAPI Swagger specification.
- `EdFiClient.resources` and `EdFiClient.descriptors` lazily retrieves lists of respective endpoints from Swagger.
- `EdFiEndpoint` child class attributes `description` and `has_deletes` lazily retrieves this metadata from Swagger.


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
