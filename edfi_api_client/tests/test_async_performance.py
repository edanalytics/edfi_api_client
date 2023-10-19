import easecret
import os
import time

from edfi_api_client import EdFiClient


###
master_secret = "edfi_scde_2023"


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

    # Map resources to exact change versions to extract N * 1000 rows.
    # Source: `edfi_scde_2023`
    max_change_versions = {
        "students": {
            20 : 491831,
            40 : 945701,
            80 : 1596723,
            160: 2928422,
            320: 4649473,
            640: 11579986,
        },
        "studentSectionAttendanceEvents": {
            20 : 70738320,
            40 : 70825225,
            80 : 70926490,
            160: 71174025,
            320: 72151990,
            640: 73499849,
        },
    }
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

    for resource, cv_row_counts in max_change_versions.items():
        for k_row_count, max_change_version in cv_row_counts.items():

            endpoint = edfi.resource(resource, minChangeVersion=0, max_change_version=max_change_version)
            endpoint_count = endpoint.total_count()

            for pool_size in pool_sizes:
                print(f"Resource: {resource}; Num rows: {k_row_count}k; Pool size: {pool_size}")

                async_kwargs.update(pool_size=pool_size)

                # Reset the output to ensure data has been written each run.
                if os.path.exists(output_path):
                    os.remove(output_path)

                runtime, _ = time_it(endpoint.async_get_to_json, **async_kwargs)

                # Get row count of written file.
                async_count = sum(1 for _ in open(output_path))
                if async_count != endpoint_count:
                    print("    Number of extracted rows did not match!")

                print(f"    Runtime: {runtime} seconds")
