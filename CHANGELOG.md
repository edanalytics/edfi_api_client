# edfi_api_client v0.3.0
## New Features
- Remove support for Ed-Fi 2. 
- Add optional token caching to persist auth tokens to disk using new `token_cache` argument.

## Under the Hood
- Migrate REST functionality into its own `Session` class:
  - Initializes lazily and only authenticates when interfacing with an authenticated API call
  - Allow exponential backoff for any Session method call.
  - Add internal methods for POSTs, DELETEs, and PUTs.
- Replace `verbose_log` print statements with the built-in `logging` library.
- Deprecate `EdFiEndpoint.total_count()` in favor of more descriptive `get_total_count()`.
- Add authentication and caching tests to package.

## Fixes
- Fix behavior of `camel_to_snake()` util helper to handle more resource names.


# edfi_api_client v0.2.3
## New Features
- Add `use_snapshot` flag to `EdFiClient` for making requests against snapshots (default `False`).


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
