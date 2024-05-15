# edfi_api_client v0.3.0
## New Features
- Add URL property methods to `EdFiClient`.
- Add Swagger definitions parsing to `EdFiSwagger`.
    - Add `EdFiEndpoint.definition` and `EdFiEndpoint.validate()` methods.
    - Add `EdFiEndpoint.{fields, field_dtypes, required_fields, identity_fields}` properties.
- Add `EdFiEndpoint.get_to_json()` getter to stream records to a file.
- Add POST, DELETE, and PUT methods to `EdFiEndpoint`.
- Add async counterparts for all `EdFiEndpoint` rest methods.

## Under the hood
- Remove support for Ed-Fi 2.0.
- Make pagination predictable by building and iterating param windows, instead of paging dynamically.
    - `EdFiComposite.get_pages()` still uses dynamic logic, since `Total-Count` header is not enabled for composites.
- Standardize verbose logging using built-in `logging` library.
- Add retry-arguments to `EdFiClient.connect()`.
- Make `EdFiSwagger` lazy to only pull Swagger when required.
- Deprecate `EdFiEndpoint.total_count()` in favor of `EdFiEndpoint.get_total_count()` for consistency.
- Check and re-authenticate automatically before all authenticated session calls.
- Standardize retry status codes and retry logic.


# edfi_api_client v0.2.1
## Under the hood
- Move several functions from `EdFiResource` to `EdFiEndpoint` to simplify inheritence between classes.
- Genericize verbose endpoint logging to change depending on `type` of endpoint class.
- Power `EdFiEndpoint.ping()` and `.total_count()` with internal helpers for improved logging.

## Fixes
- Make `EdFiEndpoint` class attributes into instance attributes
- Fix bug in `EdFiComposite.get_pages()` where pages were not yielded


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
