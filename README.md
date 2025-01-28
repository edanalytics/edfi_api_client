# Ed-Fi API Client Python Package

Note: As of package version `0.3.x`, support for Ed-Fi2 has been removed.

## Quick Guide

```python
from edfi_api_client import EdFiClient

# Client connection with Ed-Fi3 ODS
api = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET, api_version=3)

# Get the total row-count for the 'students' resource in the ODS
students = api.resource('students')
students.get_total_count()

# Pull all rows for the 'staffs' resource deletes endpoint (setting a custom page-size)
staffs = api.resource('staffs', get_deletes=True)
for row in staffs.get_rows(page_size=500):
    pass

# Pull all rows for the 'studentStaffAssociations' resource as pages (retrying when given authentication-timeout errors)
ssa = api.resource('studentStaffAssociations')  # OR 'student_staff_associations'
for page in ssa.get_pages(retry_on_failure=True):
    pass
    
# Pull all rows for the enrollment students composite, filtering by section ID
enrollment_students = api.composite('students', filter_type='sections', filter_id='12345')
for row in enrollment_students.get_rows():
    pass
```

------


## EdFiClient
`EdFiClient` serves as the interface with the ODS.
If credentials are provided, a session with the ODS is automatically authenticated.
Some methods do not require credentials to be called.

<details>
<summary>Arguments:</summary>

-----

| Argument      | Description                                                                                                                                                            |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base_url      | [Required] The root url of the API server, without any trailing components like `data/v3` or `api/v2.0`                                                                |
| client_key    | The key                                                                                                                                                                |
| client_secret | The secret                                                                                                                                                             |
| api_version   | the suite number of the API (Default `3`: Ed-Fi 2 functionality has been deprecated)                                                                                   |
| api_mode      | The API mode of the ODS (e.g., `shared_instance`, `year_specific`, etc.). If empty, the mode will automatically be inferred from the ODS' Swagger spec (Ed-Fi 3 only). |
| api_year      | The year of data to connect to if accessing a `year_specific` or `instance_year_specific` ODS.                                                                         |
| instance_code | The instance code if accessing an `instance_year_specific` ODS.                                                                                                        |
| verify_ssl    | Whether to verify SSL when making requests (Default `True`)                                                                                                            |
| verbose       | Whether to log debug information during requests (Default `False`)                                                                                                     |

If either `client_key` or `client_secret` are empty, a session with the ODS will not be established.

-----

</details>


All code examples in this document use verbose-logging to more-explicitly show interactions with the API.
It is recommended to set `verbose=True` while working interactively with the API.

```python
>>> from edfi_api_client import EdFiClient
>>> api = EdFiClient(BASE_URL, verbose=True)
Client key and secret not provided. Connection with ODS will not be attempted.

# OR

>>> from edfi_api_client import EdFiClient
>>> api = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET, verbose=True)
Connection to ODS successful!
```

### Attributes

Authentication with the ODS is not required:

<details>
<summary><code>resources</code></summary>

-----

### resources
Retrieve a list of namespaced-resources from the `resources` Swagger payload.

```python
>>> api.resources
[('ed-fi', 'academicWeeks'), ('ed-fi', 'accounts'), ('ed-fi', 'accountCodes'), ...]
```

-----

</details>


<details>
<summary><code>descriptors</code></summary>

-----

### descriptors
Retrieve a list of namespaced-descriptors from the `descriptors` Swagger payload.

```python
>>> api.descriptors
[('ed-fi', 'absenceEventCategoryDescriptors'), ('ed-fi', 'academicHonorCategoryDescriptors'), ...]
```
-----

</details>


<details>
<summary><code>oauth_url</code></summary>

-----

### oauth_url
Build the URL used in authentication

```python
>>> api.oauth_url
'{BASE_URL}/oauth/token'
```
-----

</details>


<details>
<summary><code>resource_url</code></summary>

-----

### resource_url
Build the URL used when interfacing with Ed-Fi resource endpoints

```python
>>> api.resource_url
'{BASE_URL}/data/v3'
```
-----

</details>


<details>
<summary><code>composite_url</code></summary>

-----

### composite_url
Build the URL used when interfacing with Ed-Fi composite endpoints

```python
>>> api.composite_url
'{BASE_URL}/composites/v1'
```
-----

</details>


<details>
<summary><code>change_version_url</code></summary>

-----

### change_version_url
Build the URL used when interfacing with the Ed-Fi change-queries endpoint

```python
>>> api.change_version_url
'{BASE_URL}/changeQueries/v1'
```
-----

</details>


<details>
<summary><code>resources_swagger</code></summary>

-----

### resources_swagger
Lazy `EdFiSwagger` object for the "resources" endpoints.
Populates only when attributes are called.

```python
>>> api.resources_swagger
<Ed-Fi Resources OpenAPI Swagger Specification>
```
-----

</details>


<details>
<summary><code>descriptors_swagger</code></summary>

-----

### descriptors_swagger
Lazy `EdFiSwagger` object for the "descriptors" endpoints.
Populates only when attributes are called.

```python
>>> api.descriptors_swagger
<Ed-Fi Descriptors OpenAPI Swagger Specification>
```
-----

</details>


<details>
<summary><code>composites_swagger</code></summary>

-----

### composites_swagger
Lazy `EdFiSwagger` object for the "composites" endpoints.
Populates only when attributes are called.

```python
>>> api.composites_swagger
<Ed-Fi Composites OpenAPI Swagger Specification>
```
-----

</details>



### Methods
Authentication with the ODS is not required:

<details>
<summary><code>get_info</code></summary>

-----

### get_info
Ed-Fi3 provides an informative payload at the ODS base URL.
This contains versioning by suite and build, API mode, and URLs for authentication and data management.

```python
>>> api.get_info()
{'apiMode': 'Shared Instance',
 'build': '2022.6.1.2034',
 'dataModels': [{'name': 'Ed-Fi', 'version': '3.3.0-a'}],
 'informationalVersion': '5.2',
 'suite': '3',
 'urls': {'dataManagementApi': '{BASE_URL}/data/v3/',
          'dependencies': '{BASE_URL}/metadata/data/v3/dependencies',
          'oauth': '{BASE_URL}/oauth/token',
          'openApiMetadata': '{BASE_URL}/metadata/',
          'xsdMetadata': '{BASE_URL}/metadata/xsd'},
 'version': '5.2'}
```

-----

</details>


<details>
<summary><code>get_api_mode</code></summary>

-----

### get_api_mode
Each Ed-Fi3 ODS has a declared API mode that alters how users interact with the ODS. This is a shortcut-method for finding the API mode of the Ed-Fi ODS via the payload retrieved using `EdFiClient.get_info()`, formatted in snake_case.

```python
>>> api.get_api_mode()
'shared_instance'
```
This method is called automatically when `api_mode` is left undefined by the user.

</details>


<details>
<summary><code>get_ods_version</code></summary>

### get_ods_version
This is a shortcut-method for finding the version of the Ed-Fi ODS via the payload retrieved using `EdFiClient.get_info()`.

```python
>>> api.get_ods_version()
'5.2'
```

-----

</details>


<details>
<summary><code>get_data_model_version</code></summary>

-----

### get_data_model_version
This is a shortcut-method for finding the data model version of the Ed-Fi ODS' 'ed-fi' namespace via the payload retrieved using `EdFiClient.get_info()`.

```python
>>> api.get_data_model_version()
'3.3.0-a'
```

-----

</details>


<details>
<summary><code>get_instance_locator</code></summary>

-----

### get_instance_locator
Construct an instance locator to append to the base URL in a multi-ODS.

```python
>>> api.get_instance_locator()
'2024'
```

-----

</details>


<details>
<summary><code>get_swagger</code></summary>

-----

### get_swagger
The entire Ed-Fi API is outlined in an OpenAPI Specification (i.e., Swagger Specification).
There is a separate Swagger defined for each component type (e.g., resources, descriptors, etc.).

If `component` is unspecified, `resources` will be collected.

```python
>>> swagger = api.get_swagger(component='resources')  # Default
>>> swagger
<Ed-Fi Resources OpenAPI Swagger Specification>

>>> swagger.json
{'swagger': ...,
 'basePath': ...,
 'consumes': ...,
 'definitions': ...,
 ...}
```

Returns an `EdFiSwagger` class containing the complete JSON payload, as well as extracted metadata from the Swagger.

-----

</details>


<details>
<summary><code>is_edfi2</code></summary>

-----

### is_edfi2
Deprecated in version 0.3. Always returns `False`.

```python
>>> api.is_edfi2()
False
```

-----

</details>


Authentication with the ODS is required:

<details>
<summary><code>connect</code></summary>

-----

### connect
This method initiates and authenticates a connection to the ODS.
When `client_key` and `client_secret` is passed in `EdFiClient` initialization, a non-retry client is connected automatically.

All requests made after connection will use retry-logic as defined in optional connection arguments.

```python
>>> api.connect(
        retry_on_failure=False  # If True, use exponential backoff while making requests
        max_retries=5,          # Default; number of retries to attempt before giving up on a request
        max_wait=1200           # Default; maximum time to wait between retries before giving up on a request
    )

[EdFiSession]
```

If `EdFiClient.connect()` is used as a context manager, the session will be closed automatically.
```python
with api.connect():
    ...

# The session is closed and `EdFiClient.connect()` must be run again to make requests.
```

-----

</details>


<details>
<summary><code>get_newest_change_version</code></summary>

-----

### get_newest_change_version
This method requires a connection to the ODS.

Starting in Ed-Fi3, each row in the ODS is linked to an ODS-wide "change version" parameter, which allows for narrow time-windows of data to be filtered for delta-ingestions, instead of only full-ingestions.
This method returns the newest change version defined in the ODS.

```python
>>> api.get_newest_change_version()
59084739
```

-----

</details>


<details>
<summary><code>resource</code></summary>

-----

### resource
This method requires a connection to the ODS.

Use this method to initialize an EdFiResource (i.e. EdFiEndpoint).
This object contains methods to pull rows and resource metadata from the API.

```python
>>> api.resource(
        name='students',        # Name of resource
        namespace='ed-fi',      # Default ; custom resources use a different namespace
        get_deletes=False,      # Default ; set to `True` to access the /deletes endpoint (mutually-exclusive to `get_key_changes`)
        get_key_changes=False,  # Default ; set to `True` to access the /keyChanges endpoint (mutually-exclusive to `get_deletes`)
        params={},              # Optional; used to set default parameters in API calls
        **kwargs                # Optional; alternative way to pass default parameters to API calls
    )

<Resource [edFi/students]>
```
`name`, `params`, and `kwargs` can be formatted in **snake_case** or **camelCase**.

Alternatively, a `(namespace, name)` tuple can be passed as the first argument (to reflect the output of `EdFiClient.resources`).

-----

</details>


<details>
<summary><code>descriptor</code></summary>

-----

### descriptor
This method requires a connection to the ODS.

Use this method to initialize an EdFiResource (i.e. EdFiEndpoint).
This object contains methods to pull rows and descriptor metadata from the API.

Note that although descriptors and resources are saved at the same endpoint in the ODS, descriptors do not use their /deletes endpoint.

```python
>>> api.descriptor(
        name='sexDescriptors',  # Name of descriptor
        namespace='ed-fi',      # Default ; custom resources use a different namespace
        params={},              # Optional; used to set default parameters in API calls
        **kwargs                # Optional; alternative way to pass default parameters to API calls
    )

<Resource [edFi/sexDescriptors]>
```
`name`, `params`, and `kwargs` can be formatted in **snake_case** or **camelCase**.

Alternatively, a `(namespace, name)` tuple can be passed as the first argument (to reflect the output of `EdFiClient.descriptors`).

-----

</details>


<details>
<summary><code>composite</code></summary>

-----

### composite
This method requires a connection to the ODS.

Use this method to initialize an EdFiComposite (i.e. EdFiEndpoint).
This object contains methods to pull rows and composite metadata from the API.

Note: The only composite currently defined in the API is `enrollment`.

```python
>>> api.composite(
        name='students',         # Name of composite resource
        namespace='ed-fi',       # Default ; custom resources use a different namespace
        composite='enrollment',  # Default ; name of composite
        filter_type=None,        # Optional; used to filter composites by ID and type
        filter_id=None,          # Optional; used to filter composites by ID and type
        params={},               # Optional; used to set default parameters in API calls
        **kwargs                 # Optional; alternative way to pass default parameters to API calls
    )

<Enrollment Composite [edFi/students]>
```
`name`, `params`, and `kwargs` can be formatted in **snake_case** or **camelCase**.

-----

</details>

------


## EdFiEndpoint
`EdFiEndpoint` is an abstract base class for interfacing with API endpoints.
All methods that return `EdFiEndpoint` and child classes require an authentication session with the API.

```python
>>> students = api.resource('students', min_change_version=52028375, max_change_version=53295015)
>>> students
<Resource with 2 parameters [edFi/students]>

# AND/OR

>>> students_composite = api.composite('students')
>>> students_composite
<Enrollment Composite [edFi/students]>
```

### Attributes

<details>
<summary><code>raw</code></summary>

-----

### raw
This attribute retrieves the Ed-Fi endpoint's raw camelCase representation.

```python
>>> api.resource('bell_schedules').raw
'edFi/bellSchedules'
```
-----

</details>


<details>
<summary><code>url</code></summary>

-----

### url
This attribute retrieves the Ed-Fi endpoint's url without any REST identifiers.

```python
>>> api.resource('bell_schedules').url
'{BASE_URL}/data/v3/ed-fi/bellSchedules'
```
-----

</details>


<details>
<summary><code>description</code></summary>

-----

### description
This attribute retrieves the Ed-Fi endpoint's description if present in its respective Swagger payload.

```python
>>> api.resource('bellSchedules').description
'This entity represents the schedule of class period meeting times.'
```


-----

</details>


<details>
<summary><code>has_deletes</code></summary>

-----

### has_deletes
This attribute returns whether a deletes path is present in the Ed-Fi endpoint's respective Swagger payload.

```python
>>> api.resource('bellSchedules').has_deletes
True
```

-----

</details>


<details>
<summary><code>fields</code></summary>

-----

### fields
This attribute returns the fields listed for the Ed-Fi endpoint if present in its respective Swagger payload.

Note that the private `id` and `_etag` fields are removed from the output.

```python
>>> api.resource('bellSchedules').fields
['schoolReference', 'startTime', 'alternateDayName', 'endTime', 'bellScheduleName', 'dates', ...]
```

-----

</details>


<details>
<summary><code>required_fields</code></summary>

-----

### required_fields
This attribute returns the required fields listed for the Ed-Fi endpoint if present in its respective Swagger payload.

```python
>>> api.resource('bellSchedules').required_fields
['bellScheduleName', 'classPeriods', 'schoolReference']
```

-----

</details>


### Methods

<details>
<summary><code>ping</code></summary>

-----

### ping
This method pings the endpoint and returns a Response object with scrubbed JSON data.
This offers a shortcut for verifying claim-set permissions without needing to pull data from the ODS.

```python
>>> res = students.ping()
>>> res
<Response [200]>

>>> res.json()
{'message': 'Ping was successful! ODS data has been intentionally scrubbed from this response.'}
```

-----

</details>


<details>
<summary><code>get</code></summary>

-----

### get
This method retrieves one GET-request of JSON rows from the specified endpoint.
This can be used to verify the structure of the data or to collect a small sample for testing.

An optional `limit` can be provided.
If unspecified, the default limit will be retrieved.
(This value must be less than the hard-coded limit of the ODS, or the request will fail.)

An optional `params` dictionary can be provided that overrides the default params specified during endpoint initialization.

```python
>>> students.get(limit=1)
[Get Resource] Endpoint  : {BASE_URL}/data/v3/ed-fi/students
[Get Resource] Parameters: {}
[{'id': 'abc123', 'studentUniqueId': '987654', 'birthDate': '1970-01-01', ...}]
```
Because this GET does not use pagination, the return is a list, not a generator.

-----

</details>


<details>
<summary><code>get_rows / get_pages / get_to_json</code></summary>

-----

### get_rows / get_pages / get_to_json
These are the primary methods for retrieving all JSON rows from the specified endpoint and parameters.
The only difference in function is whether the rows are returned individually or in batches (i.e., pages).
Iteration continues until no rows are returned.

All methods use identical arguments.
Method `get_to_json()` takes an additional `path` argument.
Under the hood, `get_rows()` implements `get_pages()`, but unnests the rows before returning.

An optional `params` dictionary can be provided that overrides the default params specified during endpoint initialization.

```python
>>> student_rows = students.get_rows(
        page_size=500,           # The limit to pass to the parameters. Overwrites parameter if already defined.
        retry_on_failure=False,  # Reattempt request during failures. Overrides defaults set in `EdFiClient.connect()`.
        max_retries=5,           # If `retry_on_failure is True`, how many attempts before giving up. Overrides defaults set in `EdFiClient.connect()`.
        max_wait=500,            # If `retry_on_failure is True`, max wait time for exponential backoff before giving up. Overrides defaults set in `EdFiClient.connect()`.
    
        step_change_version=False,       # Only available for resources/descriptors. See [Change Version Stepping] below.
        change_version_step_size=50000,  # Only available for resources/descriptors. See [Change Version Stepping] below.
        reverse_paging: bool=True        # Only available for resources/descriptors. See [Change Version Stepping] below.
    )
<generator object EdFiEndpoint.get_rows at 0x7f7472650f90>

>>> list(student_rows)
[Get Resource] Endpoint  : {BASE_URL}/data/v3/ed-fi/students
[Get Resource] Parameters: {'minChangeVersion': 52028375, 'maxChangeVersion': 53295015, 'limit': 500, 'offset': 0}
[Get Resource] @ Retrieved 500 rows. Paging offset...
# ...
[Get Resource] Parameters: {'minChangeVersion': 52028375, 'maxChangeVersion': 53295015, 'limit': 500, 'offset': 4000}
[Get Resource] @ Retrieved 135 rows. Paging offset...
[Get Resource] @ Retrieved zero rows. Ending pagination.
[{'id': 'abc123', 'studentUniqueId': '987654', 'birthDate': '1970-01-01', ...}, ...]
```
To circumvent memory constraints, these methods return generators instead of lists.

-----

</details>


<details>
<summary><code>get_total_count</code></summary>

-----

### get_total_count
This method returns the total count of rows for the given endpoint, as declared by the API.
This action is completed by sending a limit 0 GET request to the API with the `Total-Count` header set to `True`.

An optional `params` dictionary can be provided that overrides the default params specified during endpoint initialization.

```python
>>> students.get_total_count()
4135
```

`get_total_count()` is currently only implemented for resources and descriptors, not composites.

This method was previously named `total_count()`; this namespace has been deprecated in favor of `get_total_count()`.

-----

</details>


<details>
<summary><code>post</code></summary>

-----

### post
This method post one JSON row to the specified endpoint, returning the status code and message of the response.
This can be used to verify the structure of the data or to run a small sample for testing.

```python
>>> students.post(json_row)
(200, "")
```

Posts are currently only implemented for resources and descriptors, not composites.

-----

</details>


<details>
<summary><code>post_rows / post_from_json</code></summary>

-----

### post_rows / post_from_json
These are the primary methods for posting a set of JSON records to the specified endpoint.
Method `post_rows()` takes an iterator; method `post_from_json()` streams records to post from a JSON file.

Both methods return a `ResponseLog` mapping that documents each response's status code and message with the line numbers that returned that response.

Both methods use identical arguments.
Method `post_rows()` uses a positional `rows` argument;
method `post_from_json()` uses a positional `path` argument.

Method `post_from_json()` has two optional arguments `include` and `exclude` can be specified to post a subset of rows, based on index in the file.
If left blank, all rows in the file are posted.
These arguments are mutually-exclusive.

```python
>>> output_dict = students.post_rows(
        rows=json_rows,
        log_every=500,           # After how many requests should a debug status be printed?
        retry_on_failure=False,  # Reattempt request during failures. Overrides defaults set in `EdFiClient.connect()`.
        max_retries=5,           # If `retry_on_failure is True`, how many attempts before giving up.  Overrides defaults set in `EdFiClient.connect()`.
        max_wait=500,            # If `retry_on_failure is True`, max wait time for exponential backoff before giving up.  Overrides defaults set in `EdFiClient.connect()`.
    )

>>> output_dict
{'200': [1, 2, 3], '401: MESSAGE': [4, 5]}
```

Posts are currently only implemented for resources and descriptors, not composites.

-----

</details>


<details>
<summary><code>delete</code></summary>

-----

### delete
This method deletes one ID from the specified endpoint, returning the status code and message of the response.
This can be used to run a small sample for testing.

```python
>>> students.delete(20)
(200, "")
```

Deletes are currently only implemented for resources and descriptors, not composites.

-----

</details>


<details>
<summary><code>delete_ids</code></summary>

-----

### delete_ids
This is the primary method for deleting a set of JSON records from the specified endpoint.

This method returns a `ResponseLog` mapping that documents each response's status code and message with the indexes that returned that response.

```python
>>> output_dict = students.delete_ids(
        ids=record_ids,          # An iterator with IDs to be deleted from the endpoint
        log_every=500,           # After how many requests should a debug status be printed?
        retry_on_failure=False,  # Reattempt request during failures. Overrides defaults set in `EdFiClient.connect()`.
        max_retries=5,           # If `retry_on_failure is True`, how many attempts before giving up.  Overrides defaults set in `EdFiClient.connect()`.
        max_wait=500,            # If `retry_on_failure is True`, max wait time for exponential backoff before giving up.  Overrides defaults set in `EdFiClient.connect()`.
    )

>>> output_dict
{'200': [1, 2, 3], '401: MESSAGE': [4, 5]}
```

Deletes are currently only implemented for resources and descriptors, not composites.

-----

</details>


<details>
<summary><code>put</code></summary>

-----

### put
This method updates the payload for a given ID at the specified endpoint, returning the status code and message of the response.
This can be used to verify the structure of the data or to run a small sample for testing.

```python
>>> students.put(20, json_row)
(200, "")
```

Puts are currently only implemented for resources and descriptors, not composites.

-----

</details>


<details>
<summary><code>put_id_rows</code></summary>

-----

### put_id_rows
This is the primary method for updating a set of JSON records by ID at the specified endpoint.

This method returns a `ResponseLog` mapping that documents each response's status code and message with the indexes that returned that response.

```python
>>> output_dict = students.put_id_rows(
        id_rows=id_to_json_rows, # An iterator with ID-JSON row tuples to be updating at the endpoint
        log_every=500,           # After how many requests should a debug status be printed?
        retry_on_failure=False,  # Reattempt request during failures. Overrides defaults set in `EdFiClient.connect()`.
        max_retries=5,           # If `retry_on_failure is True`, how many attempts before giving up.  Overrides defaults set in `EdFiClient.connect()`.
        max_wait=500,            # If `retry_on_failure is True`, max wait time for exponential backoff before giving up.  Overrides defaults set in `EdFiClient.connect()`.
    )

>>> output_dict
{'200': [1, 2, 3], '401: MESSAGE': [4, 5]}
```

Puts are currently only implemented for resources and descriptors, not composites.

-----

</details>


------


## Advanced Usage: Asynchronous Requests
Ed-Fi API Client version 0.3 adds asynchronous alternatives to all `EdFiEndpoint` request methods.
These act identically to their synchronous counterparts, but complete processes faster by starting new requests while waiting on the ODS.

Asynchronous processing is more complex and requires deeper understanding of Python to use effectively.
It is highly recommended to use and understand the synchronous methods before attempting their asynchronous counterparts.

Using the asynchronous methods requires additional packages be installed in your Python environment.
Use the following command to install these packages: `pip install edfi_api_client[async]`.

### Connecting to an asynchronous session
There are two ways to connect to an asynchronous session.
The standard approach is `EdFiClient.async_connect()`.

<details>
<summary><code>EdFiClient.async_connect</code></summary>

-----

### EdFiClient.async_connect
This method is identical to `EdFiClient.connect()`,
but with one additional argument `pool_size` that determines how many asynchronous processes can run simultaneously.

Unlike its synchronous counterpart, this method _must_ be used as an asynchronous context manager!

```python
import asyncio
from edfi_api_client import EdFiClient

async def main():
    api = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET)
    
    async with api.async_connect(
        pool_size=8,  # The number of asynchronous processes to run.
        **kwargs      # Arguments used in `EdFiClient.connect()`
    ):
        ...  # Asynchronous gets/posts/deletes/puts

asyncio.run(main())
```

Any asynchronous context managers must be declared within an asynchronous method that is passed to `asyncio.run()`.
Use this approach when you want the most control when using asynchronous processing.

-----

</details>

Alternatively, most of the asynchronous request methods establish a session automatically if not already defined.
These can be used for one-off asynchronous method calls that have a clear definition for when they have been completed
(i.e., for when the asynchronous session can be closed).
Note that the session is reset and re-authenticated between request-methods if a session context manager is not used.

All asynchronous endpoint methods have been defined below.

<details>
<summary><code>EdFiEndpoint Asynchronous Methods</code></summary>

-----

`EdFiSession.async_connect()` arguments can be passed in these method-calls for session-instantiation when not explicitly pre-instantiated.

- async_get
- async_get_pages*
- async_get_rows*
- async_get_to_json
- async_get_total_count
- async_post
- async_post_rows
- async_post_from_json
- async_delete
- async_delete_ids
- async_put
- async_put_id_rows

Methods that will *not* automatically create an asynchronous session are marked with an asterisk.
These are both getters that return an `AsyncGenerator` that can often be too large to fit into memory.

For an example of how these are used without an asynchronous context manager:
```python
from edfi_api_client import EdFiClient

api = EdFiClient(BASE_URL, CLIENT_KEY, CLIENT_SECRET)
students = api.resource('students')

students.async_get_to_json(PATH, retry_on_failure=True, pool_size=8)  # Creates a new asynchronous retry-Session with pool-size 8.
students.async_post_from_json(PATH, pool_size=16)  # Creates a different asynchronous Session with pool-size 16.
```

-----

</details>


------


## Change Version Stepping
The Ed-Fi API already has pagination built-in via the `limit` and `offset` parameters passed in GET-requests.

Here is an example of what calls to the API look like using pagination (page size 500), charted across time by change versions.
![EdFiPagination](https://github.com/edanalytics/edfi_api_client/raw/main/images/edfi_api_pagination.gif)

This client provides a second type of pagination that uses change versions to improve performance when pulling from the API, referred to here as _change version stepping_.

A change version window of a specified length is defined, and calls to the API pass the min and max change versions of this window.
Ordinary pagination still occurs within each window until zero rows are returned, after which the change version window steps and the process is repeated.

Here is an example of what calls to the API look like using change version stepping (step-window size 2000 and page size 500).
![EdFiChangeVersionStepping](https://github.com/edanalytics/edfi_api_client/raw/main/images/edfi_api_changeversion.gif)


Note that change versions are currently accessible only for **resources**, not for composites.

<details>
<summary>Why is change version stepping recommended when pulling from the API?</summary>

-----

We can imagine requests sent to the Ed-Fi API as SQL select statements against the underlying ODS.
For example, the code below makes repeated calls to the API, paging by 500 until all rows are retrieved.
```python
>>> students = api.resource('students', schoolYear='2022')
>>> students.get_rows(page_size=500)
```

This code is semantically identical to the following SQL statements:
```sql
SELECT * FROM students WHERE schoolYear = '2022' LIMIT 500 OFFSET 0;
SELECT * FROM students WHERE schoolYear = '2022' LIMIT 500 OFFSET 500;
SELECT * FROM students WHERE schoolYear = '2022' LIMIT 500 OFFSET 1000;
-- etc.
```

This works fine for small-volume resources.
However, as `offset` increases, the computational-runtime of the query increases with it.
For large-volume resources (e.g. `studentSectionAttendanceEvents`), this could translate to the following:
```sql
SELECT * FROM studentSectionAttendanceEvents LIMIT 500 OFFSET 100000000;
```

This is the equivalent of calculating the first 100,000,500 rows of data, but only collecting the final 500.
In practice, the connection to the ODS will time-out and need to re-authenticated before this query returns.

Luckily, the Ed-Fi3 "change versions" feature provides a helpful workaround for this.
By specifying a min- and max-change-version in the query, a filtered select is applied that never reaches high offset.
```python
>>> students = api.resource('students', min_change_version=0, max_change_version=50000)
>>> students.get_rows(page_size=500)
```
By definition, a change-version window will never contain more rows than the size of that window.
Therefore, because the change version window defined above is only 50000 (i.e., `max_change_version - min_change_version`),
the final API-call will be equivalent to the following:
```sql
SELECT * FROM students WHERE changeVersion BETWEEN 0 AND 50000 LIMIT 500 OFFSET 50000
```

-----

</details>

Setting `step_change_version = True` in `get_rows()` or `get_pages()` turns on change version stepping.
Use `change_version_step_size` to set the width of each stepping window (default 50000).

```python
>>> students = api.resource('students', min_change_version=52028375, max_change_version=53295015)
>>> student_rows = students.get_rows(
        page_size=500,
        step_change_version=True,
        change_version_step_size=50000  # Default value. This is NOT optimized. Raise it to reduce API calls.
    )

>>> list(student_rows)
[Paged Get Resource] Endpoint  : {BASE_URL}/data/v3/ed-fi/students
[Paged Get Resource] Parameters: {'minChangeVersion': 52028375, 'maxChangeVersion': 52078375, 'limit': 500, 'offset': 0}
[Paged Get Resource] @ Retrieved 101 rows. Paging offset...
[Paged Get Resource] Parameters: {'minChangeVersion': 52028375, 'maxChangeVersion': 52078375, 'limit': 500, 'offset': 500}
[Paged Get Resource] @ Retrieved zero rows. Stepping change version...
[Paged Get Resource] Parameters: {'minChangeVersion': 52078376, 'maxChangeVersion': 52128375, 'limit': 500, 'offset': 0}
[Paged Get Resource] @ Retrieved 500 rows. Paging offset...
# ...
[Paged Get Resource] Parameters: {'minChangeVersion': 53278376, 'maxChangeVersion': 53295015, 'limit': 500, 'offset': 0}
[Paged Get Resource] @ Retrieved zero rows. Stepping change version...
[Paged Get Resource] @ Change version exceeded max. Ending pagination.
```

To ingest all rows for a resource, find the ODS' newest change version and apply this to `max_change_version`, as below:
```python
>>> max_change_version = api.get_newest_change_version()

>>> students = api.resource('students', min_change_version=0, max_change_version=max_change_version)
>>> students.get_rows(page_size=500, step_change_version=True)
```

**Things to note when using change version stepping:** 
* Change version stepping usually results in more requests made to the API; however, they are far less likely to overwhelm it as with high offsets.
* Using change version stepping requires both `min_change_version` and `max_change_version` be defined within either the resource's `params` or `kwargs`.
If either are undefined, an error is raised.
* The default `change_version_step_size` is set to `50000`.
This value is not optimized. Try raising it to send fewer requests to the API.
* API De-synchronization can occur when using Change-Version Stepping. See [Reverse Paging](#reverse-paging).


## Reverse Paging

There is a known problem that can occur when pulling from the API using change-version limits and without snapshotting.
If any rows within the change-version window are updated mid-pull, their change-version is updated and they escape the window.
When this occurs, all other rows in the window shift to fill the place of the missing row, resulting in rows entering previously pulled limit-offset pages and being missed in subsequent calls to the API.
This leads to a gradual de-synchronization between the API and datalakes built from the API.



We have added a new offset-pagination method to counteract this bug, known as "reverse paging."
By default, when `step_change_version=True` in resource pulls, requests are made to the API starting at the greatest offset and iterating backwards until offset zero.
If a row is updated and a shift occurs mid-pull, one or more rows in the change version may be ingested multiple times, but no rows will be lost altogether.

<details>
<summary>For example:</summary>

-----

Say there are 15 rows in the `students` resource with change versions between 0 and 20.
We pull these rows using a page-size of 4.

![EdFiDesync1](https://github.com/edanalytics/edfi_api_client/raw/main/images/changeversion_desync1.png)

Say that before our fourth (and final) API call, record number 6 is updated and leaves the change-version window.
Records 7 through 15 will shift to fill its place.
When this occurs, record number 13 will shift from page 4 into a page that has already been ingested.
Therefore, it will be missed from the final output.

![EdFiDesync2](https://github.com/edanalytics/edfi_api_client/raw/main/images/changeversion_desync2.png)
 
Using reverse-paging, page 4 will be ingested first. When record number 6 is updated and the rows shift, record 13 will move into page 3 and will be ingested a second time.
However, this row will not be lost.

-----

</details>
