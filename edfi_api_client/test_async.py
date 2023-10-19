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

    max_change_versions = [
          491831,  #  20000 rows
          945701,  #  40000 rows
         1596723,  #  80000 rows
         2928422,  # 160000 rows
         4649473,  # 320000 rows
        11579986,  # 640000 rows
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
