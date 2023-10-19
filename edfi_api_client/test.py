import easecret
import os
import time

from edfi_api_client import EdFiClient


###
master_secret = "edfi_scde_2024"

def test_client(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    base_url = credentials.get('base_url')


    ##### Unauthorized Client
    edfi = EdFiClient(base_url)

    ### Info Payload
    info_payload = edfi.get_info()
    payload_keys = ('apiMode', 'version', 'dataModels', 'urls',)
    assert all(key in info_payload for key in payload_keys)

    ### Swagger
    _ = edfi.get_swagger(component='resources')
    _ = edfi.get_swagger(component='descriptors')
    _ = edfi.get_swagger(component='composites')


    ##### Authorized Client
    edfi = EdFiClient(**credentials)
    _ = edfi.get_newest_change_version()

    ### Resource
    resource = edfi.resource('students', minChangeVersion=0, maxChangeVersion=100000)
    assert resource.ping().ok

    _ = resource.description
    _ = resource.has_deletes

    resource_count = resource.total_count()
    resource_rows = resource.get_rows(page_size=500, retry_on_failure=True, step_change_version=True)
    assert len(list(resource_rows)) == resource_count

    ### Descriptor
    descriptor = edfi.resource('language_use_descriptors')
    assert descriptor.ping().ok
    _ = descriptor.description
    _ = descriptor.has_deletes

    descriptor_count = descriptor.total_count()
    descriptor_rows = descriptor.get_rows(page_size=500, step_change_version=False)
    assert len(list(descriptor_rows)) == descriptor_count

    ### Composite
    composite = edfi.composite('students')
    assert composite.ping().ok
    _ = composite.description
    _ = composite.has_deletes

    # Composites don't have total-counts.
    composite_count = 42 * 10
    composite_rows = []
    for idx, row in enumerate(composite.get_rows(page_size=100)):
        if idx == composite_count:
            break
        composite_rows.append(row)
    assert len(list(composite_rows)) == composite_count


### Helper for async time testing.
def time_it(func, *args, **kwargs):
    start = time.time()
    return_val = func(*args, **kwargs)
    end = time.time()

    runtime = round(end - start, 2)
    return runtime, return_val

def test_async(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=False)

    max_change_versions = [
          171000,  #  20078 rows
          370000,  #  40035 rows
          552500,  #  80007 rows
         2363000,  # 165282 rows
         6600000,  # 324281 rows
        25000000,  # 644500 rows
    ]
    pool_sizes = (4, 8, 16, 32,)

    scratch_dir = "./.scratch"
    os.makedirs(scratch_dir, exist_ok=True)

    output_path = os.path.join(scratch_dir, 'students_async.jsonl')
    async_kwargs = dict(
        path=output_path,
        retry_on_failure=True,
        page_size=500,
        step_change_version=True,
        change_version_step_size=10000,
    )

    for max_change_version in max_change_versions:
        students = edfi.resource('students', minChangeVersion=0, max_change_version=max_change_version)
        students_count = students.total_count()

        for pool_size in pool_sizes:
            print(f"Num rows: {students_count // 10000}0K; pool size: {pool_size}")

            async_kwargs.update(pool_size=pool_size)

            # Reset the output to ensure data has been written each run.
            if os.path.exists(output_path):
                os.remove(output_path)

            runtime, _ = time_it(students.async_get_to_json, **async_kwargs)

            # Get row count of written file.
            async_count = sum(1 for _ in open(output_path))
            if async_count != students_count:
                print("    Number of rows did not match!")

            print(f"    Runtime: {runtime} seconds")
