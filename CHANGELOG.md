# Unreleased
## New features
- New "reverse_paging" pagination method for `EdFiResource.get_pages()`

## Under the hood
- Default to reverse-paging when change-version stepping resources

## Fixes
- Fix bug in `EdFiResource.get_pages()` where default `change_version_step_size` was used instead of argument

# edfi_api_client v0.1.1
## Fixes
- retry on 500 errors

# edfi_api_client v0.1.0
Initial release